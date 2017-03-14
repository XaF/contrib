package aws

import (
	"fmt"
  "sort"
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/autoscaling"
)

const (
  localJSONFilePath = "/tmp/cluster-autoscaler-instances-info.json"
  tagSpotEnabled = "spot-enabled"
  tagOnDemandNumber = "autospotting_on_demand_number"
  tagOnDemandPercentage = "autospotting_on_demand_percentage"
  tagStorageMinSize = "autospotting_storage_min_size"
  tagStorageSSDOnly = "autospotting_storage_ssd_only"
)

type SpotInstance struct {
	SpotType      string
  SpotZone      string
  SpotPrice     float64
  MinType       string
  MaxPrice      float64
}

type SpotRequestConfiguration struct {
  MinType         string
  MaxPrice        float64
  VirtType        string
  StorageMinSize  float64
  StorageSSDOnly  bool
  InstancesMap    InstanceDataMap
}

func (asg *Asg) launchMultipleSpotInstances(
    number int) (int, error) {

  // Get full auto scaling group information
	group, err := asg.getGroupWithErr(true)
	if err != nil {
		return 0, err
	}

  // Check if there is tag rules for spotting
  spotEnabled := false
  onDemandNumber := int(0)
  onDemandPercent := float64(0)
  spotReq := &SpotRequestConfiguration{
    MaxPrice: -1,
    StorageMinSize: float64(0),
    StorageSSDOnly: false,
  }
  readInstanceConfigStorageMinSize := true
  readInstanceConfigStorageSSDOnly := true

  for _, tag := range group.Tags {
    switch *tag.Key {
    case tagSpotEnabled:
      spotEnabled = (strings.ToLower(*tag.Value) == "true")
    case tagOnDemandNumber:
      if num, err := strconv.Atoi(*tag.Value); err == nil {
        onDemandNumber = num
      }
    case tagOnDemandPercentage:
      if percent, err := strconv.ParseFloat(*tag.Value, 64); err == nil {
        onDemandPercent = percent
      }
    case tagStorageMinSize:
      if size, err := strconv.ParseFloat(*tag.Value, 64); err == nil {
        spotReq.StorageMinSize = size
        readInstanceConfigStorageMinSize = false
      }
    case tagStorageSSDOnly:
      spotReq.StorageSSDOnly = (strings.ToLower(*tag.Value) == "true")
      readInstanceConfigStorageSSDOnly = false
    }
  }

  if ! spotEnabled {
    glog.Infoln("Spotting is disabled. Falling back to normal increase.")
    return 0, nil
  }

  spawn := number
  if onDemandNumber > 0 || onDemandPercent > 0 {
    count, err := asg.getInstancesCount(group)
    if err != nil {
      return 0, err
    }

    if onDemandNumber > count["ondemand"] {
      glog.V(2).Infoln("Reducing spot spawn number as NUMBER rule is not met")
      spawn = number - (onDemandNumber - count["ondemand"])
    }

    currentOnDemandPercent :=
      float64(count["ondemand"] + number - spawn) / float64(count["total"] + number)
    if glog.V(2) && onDemandPercent / 100 > currentOnDemandPercent {
      glog.Infoln("Reducing spot spawn number as PERCENT rule is not met")
    }
    for spawn > 0 && onDemandPercent / 100 > currentOnDemandPercent {
      spawn -= 1
      currentOnDemandPercent =
        float64(count["ondemand"] + number - spawn) / float64(count["total"] + number)
    }

    glog.Infoln(spawn, "spot(s) left to spawn after applying rules")
  }

  if spawn < 1 {
    return 0, nil
  }

  // Get launch configuration information
	lc, err := asg.getLaunchConfiguration(group)
	if err != nil {
		return 0, err
	}

  // Get on demand instance information
	inst, err := asg.getInstance(true, group)
	if err != nil {
		return 0, err
	}

  // Finish initializing spot request configuration
  spotReq.MinType = *lc.InstanceType

  // Get the virtualization type used by one of the instances as reference
  spotReq.VirtType = *inst.VirtualizationType

  // Load instances information to identify the instances that could be used
  spotReq.InstancesMap, err = loadJSONInstancesDataAsMap(localJSONFilePath)
  if err != nil {
    return 0, err
  }

	// Get the current price as the cheapest from the available zones
	checkedZones := make(map[string]bool)
	for _, zoneRaw := range group.AvailabilityZones {
		zone := getRegionFromAvailabilityZone(*zoneRaw)
		if _, exists := checkedZones[zone]; ! exists {
			checkedZones[zone] = true
			priceStr := spotReq.InstancesMap[spotReq.MinType].Pricing[zone].Linux.OnDemand
			if price, err := strconv.ParseFloat(priceStr, 64);
					err == nil && (spotReq.MaxPrice < 0 || price < spotReq.MaxPrice) {
				spotReq.MaxPrice = price
			}
		}
	}

  // Update storage spot request configuration if needed
  if readInstanceConfigStorageMinSize ||
      (readInstanceConfigStorageSSDOnly && spotReq.StorageMinSize > 0) {
    storage := spotReq.InstancesMap[spotReq.MinType].Storage
    if storage != nil {
      if readInstanceConfigStorageMinSize {
        spotReq.StorageMinSize = float64(storage.Devices) * float64(storage.Size)
      }
      if readInstanceConfigStorageSSDOnly && spotReq.StorageMinSize > 0 {
        spotReq.StorageSSDOnly = storage.SSD
      }
    }
  }

  if glog.V(1) {
    glog.Infoln(
      "Spot configuration:",
      "\n\t- Min type:", spotReq.MinType,
      "\n\t- Max price:", spotReq.MaxPrice,
      "\n\t- Virtualization:", spotReq.VirtType,
      "\n\t- Storage:",
      "\n\t\t- Min size:", spotReq.StorageMinSize,
      "\n\t\t- Only SSD:", spotReq.StorageSSDOnly,
    )
  }

  nbSpawned := 0
  for i := 0; i < spawn; i++ {
    if i > 0 {
      // Refresh group information in between instances spawn to be sure
      // to have accurate instances information
    	group, err = asg.getGroupWithErr(true)
    	if err != nil {
    		return 0, err
    	}
    }
    spot, err := asg.launchSpotInstance(inst, lc, group, spotReq)
    if spot {
      nbSpawned++
    }
    if err != nil {
      return nbSpawned, err
    }
  }

  return nbSpawned, nil
}

func (asg *Asg) launchSpotInstance(
    inst *ec2.Instance,
    lc *autoscaling.LaunchConfiguration,
    group *autoscaling.Group,
    spotReq *SpotRequestConfiguration) (bool, error) {

  // Get the cheapest compatible spot instance information
	spot, err := asg.getCheapestCompatibleSpotInstance(group, spotReq)
	if err != nil {
		return false, err
	}

  // If we did not find any spot, just return false, we won't use
  // spotting in this case
  if spot == nil {
    glog.Infoln("No interesting spot found")
    return false, nil
  }

  // If we found a spot, we need to convert our basic launch
  // configuration to the launch specification of a spot
  spotLS := asg.fromLaunchConfToSpotSpec(
    group, lc, inst, spot.SpotType, spot.SpotZone)

  // Bid for the spot instance, this command will also wait
  // for the spot instance to be launched and return the
  // instance ID once it is done
  instanceId, err := asg.bidForSpotInstance(spot, spotLS, group)
  if err != nil {
    return false, err
  }

  // Wait for the instance to be in running state
  glog.Infoln("Waiting for the spot instance to be in RUNNING state")
  paramsRunning := &ec2.DescribeInstancesInput{
		InstanceIds: []*string{
			instanceId,
		},
	}

  err = asg.awsManager.ec2.WaitUntilInstanceRunning(paramsRunning)
  if err != nil {
    return true, err
  }

  // Increment the number of requested instances for the
  // auto scaling group, and attach our new spot
  glog.Infoln("Attaching spot instance to AutoScalingGroup")
  paramsAttach := &autoscaling.AttachInstancesInput{
		AutoScalingGroupName: aws.String(asg.AwsRef.Name),
		InstanceIds: []*string{
			instanceId,
		},
	}

  _, err = asg.awsManager.autoscaling.AttachInstances(paramsAttach)
  if err != nil {
    return true, err
  }

  return true, nil
}

func getRegionFromAvailabilityZone(availabilityZone string) (string) {
  lastChar := availabilityZone[len(availabilityZone)-1:]
  _, err := strconv.Atoi(lastChar)

  if err != nil {
    availabilityZone = availabilityZone[:len(availabilityZone)-1]
  }

  return availabilityZone
}

func (instancesMap InstanceDataMap) atLeastAsGoodAs(type1 string, type2 string, virtType string) (bool) {
  candidate := instancesMap[type1]
  current := instancesMap[type2]

  // Check for CPU
  if candidate.VCPU < current.VCPU {
    return false
  }

  // Check for RAM
  if candidate.Memory < current.Memory {
    return false
  }

  // Check for virtualization types
  virtType = strings.ToUpper(virtType)
  virtCompatible := false
  for _, candidateVirtType := range candidate.LinuxVirtualizationTypes {
    if (strings.ToUpper(candidateVirtType) == virtType) {
			virtCompatible = true
      break
		}
	}
  if ! virtCompatible {
    return false
  }

  // No problem was found, it's compatible
  return true
}

func (instancesMap InstanceDataMap) isBetter(type1 string, type2 string, virtType string) (bool) {
  candidate := instancesMap[type1]
  current := instancesMap[type2]

  // Check for virtualization types
  virtType = strings.ToUpper(virtType)
  virtCompatible := false
  for _, candidateVirtType := range candidate.LinuxVirtualizationTypes {
    if (strings.ToUpper(candidateVirtType) == virtType) {
			virtCompatible = true
      break
		}
	}
  if ! virtCompatible {
    return false
  }

	// Check for CPU and RAM at the same time, at least one of those must be better
	return (candidate.VCPU > current.VCPU && candidate.Memory >= current.Memory) ||
			(candidate.VCPU >= current.VCPU && candidate.Memory > current.Memory)
}

func (asg *Asg) getCheapestCompatibleSpotInstance(
    group *autoscaling.Group,
    spotReq *SpotRequestConfiguration) (*SpotInstance, error) {
  // Filter the zones in order to only search for instances in the less
  // used zones
  zonesByName := make(map[string]int)
  // Initialize map with the available zones
	for _, zoneRaw := range group.AvailabilityZones {
    zonesByName[*zoneRaw] = 0
  }
  // Increase the number of times each zone is encountered
  for _, instInfo := range group.Instances {
    zone := *instInfo.AvailabilityZone
    if val, ok := zonesByName[zone]; ok {
      zonesByName[zone] = val + 1
    } else {
      glog.Infoln("Found instance in zone", zone,
        "while it is not specified as a zone for that AutoScalingGroup")
    }
  }
  // Revert map so we can query per number of instances
  var numOfInstancesByZone []int
  zonesByNum := make(map[int][]*string)
  for zone, num := range zonesByName {
    if listOfZones, ok := zonesByNum[num]; ok {
      zonesByNum[num] = append(listOfZones, aws.String(zone))
    } else {
      zonesByNum[num] = []*string{aws.String(zone)}
      numOfInstancesByZone = append(numOfInstancesByZone, num)
    }
  }

  // Set the best price as being the current price
  bestPrice := spotReq.MaxPrice

  // Set the chosen spot type and zone as empty strings for now
  chosenSpotType := ""
  chosenSpotZone := ""

  // Treat the regions by increasing number of instances running in them
  sort.Ints(numOfInstancesByZone)
  for _, nbInstances := range numOfInstancesByZone {
    // Select the zones corresponding to the current number of instances
    zones := zonesByNum[nbInstances]
    var zonesStr []string
    for _, z := range zones {
      zonesStr = append(zonesStr, *z)
    }
    glog.Infoln("Searching in zones with", nbInstances, "instance(s):", zonesStr)

  	// Get the current spot prices
  	params := &ec2.DescribeSpotPriceHistoryInput{
  		ProductDescriptions: []*string{
  			aws.String("Linux/UNIX"),
  		},
  		StartTime: aws.Time(time.Now()),
  		EndTime: aws.Time(time.Now()),
  		Filters: []*ec2.Filter{
  			{
  				Name:   aws.String("availability-zone"),
  				Values: zones,
  			},
  		},
  	}

  	resp, err := asg.awsManager.ec2.DescribeSpotPriceHistory(params)
  	if err != nil {
  		return nil, err
  	}

  	// For each spot found...
  	for _, spot := range resp.SpotPriceHistory {
  		price, err := strconv.ParseFloat(*spot.SpotPrice, 64)
  		if err != nil {
  			// If we can't parse the price... go to the next one
        glog.Infof("Error parsing price: %v\n", err)
  			continue
  		}

  		// If the price is not better than the current one, no need to
  		// check further, we want the cheapest!
  		if price > bestPrice {
  			continue
  		}

  		// Compare to the current best instance
  		replace := false
			if chosenSpotType == "" || price < bestPrice {
				replace = spotReq.InstancesMap.atLeastAsGoodAs(
          *spot.InstanceType, spotReq.MinType, spotReq.VirtType)
			} else {
				replace = spotReq.InstancesMap.isBetter(
          *spot.InstanceType, chosenSpotType, spotReq.VirtType)
			}

      if ! replace {
        continue
      }

      // Check that the storage is compatible with what is requested
      if spotReq.StorageMinSize > 0 {
        storage := spotReq.InstancesMap[*spot.InstanceType].Storage
        if storage == nil ||
            (spotReq.StorageSSDOnly && ! storage.SSD) ||
            (float64(storage.Devices) * float64(storage.Size) < spotReq.StorageMinSize){
          continue
        }
      }

      // Check that we don't have more than 25% of this type in
      // this availability zone; skip if it is the case
      nbSpotInZone, err := asg.getSpotInstancesCount(
        *spot.InstanceType, *spot.AvailabilityZone, group)
      if err != nil {
        glog.Infof("Error counting spot instances: %v\n", err)
        continue
      }

      if nbSpotInZone > 0 {
        count, err := asg.getInstancesCount(group)
        if err != nil {
          return nil, err
        }

        if float64(nbSpotInZone) / float64(count["spot"]) > float64(.25) {
          continue
        }
      }

      // If we arrived here, just consider this spot type as
      // the current best
  		chosenSpotType = *spot.InstanceType
  		chosenSpotZone = *spot.AvailabilityZone
  		bestPrice = price
  	}

    // If we found a matching spot type, break the loop
    if chosenSpotType != "" {
      break
    }
  }

  // We did not find any interesting spot instance
  if chosenSpotType == "" {
    return nil, nil
  }

  return &SpotInstance{
    SpotType: chosenSpotType,
    SpotZone: chosenSpotZone,
    SpotPrice: bestPrice,
    MinType: spotReq.MinType,
    MaxPrice: spotReq.MaxPrice,
  }, nil
}

func (asg *Asg) getInstance(
    onDemand bool,
    group *autoscaling.Group) (*ec2.Instance, error) {

	instances_id := []*string{}
	for _, inst := range group.Instances {
		instances_id = append(instances_id, inst.InstanceId)
	}

	params := &ec2.DescribeInstancesInput{
		Filters: []*ec2.Filter{
			{
				Name: aws.String("instance-id"),
				Values: instances_id,
			},
			{
				Name: aws.String("instance-state-name"),
				Values: []*string{
					aws.String("running"),
					aws.String("pending"),
				},
			},
		},
	}

	resp, err := asg.awsManager.ec2.DescribeInstances(params)
	if err != nil {
		return nil, err
	}

	for _, res := range resp.Reservations {
		for _, inst := range res.Instances {
			if !onDemand || (onDemand && inst.InstanceLifecycle == nil) {
				return inst, nil
			}
		}
	}

	return nil, nil
}

func (asg *Asg) getInstancesCount(
    group *autoscaling.Group) (map[string]int, error) {
  count := make(map[string]int)
  count["ondemand"] = 0
  count["total"] = 0

  instances_id := []*string{}
  for _, inst := range group.Instances {
    instances_id = append(instances_id, inst.InstanceId)
  }

  params := &ec2.DescribeInstancesInput{
		Filters: []*ec2.Filter{
      {
        Name: aws.String("instance-id"),
        Values: instances_id,
      },
			{
				Name: aws.String("instance-state-name"),
				Values: []*string{
					aws.String("running"),
					aws.String("pending"),
				},
			},
		},
	}

  resp, err := asg.awsManager.ec2.DescribeInstances(params)
	if err != nil {
		return count, err
	}

  for _, res := range resp.Reservations {
    for _, inst := range res.Instances {
      instType := "ondemand"
      if inst.InstanceLifecycle != nil {
        instType = *inst.InstanceLifecycle
      }
      if num, ok := count[instType]; ok {
        count[instType] = num + 1
      } else {
        count[instType] = 1
      }
      count["total"]++
    }
  }

	return count, nil
}

func (asg *Asg) getSpotInstancesCount(
    instanceType string,
    availabilityZone string,
    group *autoscaling.Group) (int, error) {
	instances_id := []*string{}
	for _, inst := range group.Instances {
		instances_id = append(instances_id, inst.InstanceId)
	}

	params := &ec2.DescribeInstancesInput{
		Filters: []*ec2.Filter{
			{
				Name: aws.String("instance-id"),
				Values: instances_id,
			},
			{
				Name: aws.String("instance-state-name"),
				Values: []*string{
					aws.String("running"),
					aws.String("pending"),
				},
			},
			{
				Name: aws.String("instance-lifecycle"),
				Values: []*string{
					aws.String("spot"),
				},
			},
			{
				Name: aws.String("instance-type"),
				Values: []*string{
					aws.String(instanceType),
				},
			},
			{
				Name: aws.String("availability-zone"),
				Values: []*string{
					aws.String(availabilityZone),
				},
			},
		},
	}

	resp, err := asg.awsManager.ec2.DescribeInstances(params)
	if err != nil {
		return -1, err
	}

	return len(resp.Reservations), nil
}

func (asg *Asg) getLaunchConfiguration(
    group *autoscaling.Group) (*autoscaling.LaunchConfiguration, error) {

	params := &autoscaling.DescribeLaunchConfigurationsInput{
		LaunchConfigurationNames: []*string{group.LaunchConfigurationName},
	}

	resp, err := asg.awsManager.autoscaling.DescribeLaunchConfigurations(params)
	if err != nil {
		return nil, err
	}

	for _, lc := range resp.LaunchConfigurations {
		return lc, nil
	}

	return nil, nil
}

func (asg *Asg) getSubnet(
  group *autoscaling.Group,
  availabilityZone string) (*string, error) {

  subnets := strings.Split(*group.VPCZoneIdentifier, ",")

  subnetsPtr := []*string{}
  for _, subnet := range subnets {
    subnetsPtr = append(subnetsPtr, aws.String(subnet))
  }

	params := &ec2.DescribeSubnetsInput{
		SubnetIds: subnetsPtr,
		Filters: []*ec2.Filter{
			{
				Name:   aws.String("availability-zone"),
				Values: []*string{aws.String(availabilityZone)},
			},
		},
	}

	resp, err := asg.awsManager.ec2.DescribeSubnets(params)
	if err != nil {
		return nil, err
	}

  return resp.Subnets[0].SubnetId, nil
}

func (asg *Asg) fromLaunchConfToSpotSpec(
  group *autoscaling.Group,
  lc *autoscaling.LaunchConfiguration,
	inst *ec2.Instance,
	instanceType string,
	availabilityZone string) *ec2.RequestSpotLaunchSpecification {

	var spotLS ec2.RequestSpotLaunchSpecification

	// convert attributes
	spotLS.BlockDeviceMappings = copyBlockDeviceMappings(lc.BlockDeviceMappings)

	if lc.EbsOptimized != nil {
		spotLS.EbsOptimized = lc.EbsOptimized
	}

	// The launch configuration's IamInstanceProfile field can store either a
	// human-friendly ID or an ARN, so we have to see which one is it
	var iamInstanceProfile ec2.IamInstanceProfileSpecification

	if lc.IamInstanceProfile != nil {

		if strings.HasPrefix(*lc.IamInstanceProfile, "arn:aws:") {
			iamInstanceProfile.Arn = lc.IamInstanceProfile
		} else {
			iamInstanceProfile.Name = lc.IamInstanceProfile
		}

		spotLS.IamInstanceProfile = &iamInstanceProfile
	}

	spotLS.ImageId = lc.ImageId

	spotLS.InstanceType = &instanceType

	// these ones should NOT be copied, they break the SpotLaunchSpecification,
	// so that it can't be launched
	// - spotLS.KernelId
	// - spotLS.RamdiskId

	if lc.KeyName != nil && *lc.KeyName != "" {
		spotLS.KeyName = lc.KeyName
	}

	if lc.InstanceMonitoring != nil {
		spotLS.Monitoring = &ec2.RunInstancesMonitoringEnabled{
			Enabled: lc.InstanceMonitoring.Enabled,
		}
	}

	if lc.AssociatePublicIpAddress != nil || inst.SubnetId != nil {
    // Get the right subnet if we have multiple availability zones
    subnetId := inst.SubnetId
    if *inst.Placement.AvailabilityZone != availabilityZone {
      availabilityZoneSubnet, err := asg.getSubnet(group, availabilityZone)
      if err != nil {
        glog.Infof("Error while trying to find subnet: %v\n", err)
      } else {
        subnetId = availabilityZoneSubnet
      }
    }

		// Instances are running in a VPC.
		spotLS.NetworkInterfaces = []*ec2.InstanceNetworkInterfaceSpecification{
			{
				AssociatePublicIpAddress: lc.AssociatePublicIpAddress,
				DeviceIndex:              aws.Int64(0),
				SubnetId:                 subnetId,
				Groups:                   lc.SecurityGroups,
			},
		}
	} else {
		// Instances are running in EC2 Classic.
		spotLS.SecurityGroups = lc.SecurityGroups
	}

	if lc.UserData != nil && *lc.UserData != "" {
		spotLS.UserData = lc.UserData
	}

	spotLS.Placement = &ec2.SpotPlacement{AvailabilityZone: &availabilityZone}

	return &spotLS
}

func copyBlockDeviceMappings(
	lcBDMs []*autoscaling.BlockDeviceMapping) []*ec2.BlockDeviceMapping {

	var ec2BDMlist []*ec2.BlockDeviceMapping

	for _, lcBDM := range lcBDMs {
		var ec2BDM ec2.BlockDeviceMapping

		ec2BDM.DeviceName = lcBDM.DeviceName

		// EBS volume information
		if lcBDM.Ebs != nil {
			ec2BDM.Ebs = &ec2.EbsBlockDevice{
				DeleteOnTermination: lcBDM.Ebs.DeleteOnTermination,
				Encrypted:           lcBDM.Ebs.Encrypted,
				Iops:                lcBDM.Ebs.Iops,
				SnapshotId:          lcBDM.Ebs.SnapshotId,
				VolumeSize:          lcBDM.Ebs.VolumeSize,
				VolumeType:          lcBDM.Ebs.VolumeType,
			}
		}

		// it turns out that the noDevice field needs to be converted from bool to
		// *string
		if lcBDM.NoDevice != nil {
			ec2BDM.NoDevice = aws.String(fmt.Sprintf("%t", *lcBDM.NoDevice))
		}

		ec2BDM.VirtualName = lcBDM.VirtualName

		ec2BDMlist = append(ec2BDMlist, &ec2BDM)

	}
	return ec2BDMlist
}

func (asg *Asg) bidForSpotInstance(
  spot *SpotInstance,
	spotLS *ec2.RequestSpotLaunchSpecification,
  group *autoscaling.Group) (*string, error) {

	resp, err := asg.awsManager.ec2.RequestSpotInstances(
    &ec2.RequestSpotInstancesInput{
      SpotPrice: aws.String(strconv.FormatFloat(spot.MaxPrice, 'f', -1, 64)),
      LaunchSpecification: spotLS,
	  },
  )

	if err != nil {
		return nil, fmt.Errorf("Failed to create spot instance request for",
			asg.AwsRef.Name, err.Error())
	}

	req := resp.SpotInstanceRequests[0]
	glog.Infoln("Created spot instance request", *req.SpotInstanceRequestId,
    "for instance type", spot.SpotType, "in", spot.SpotZone, "- price is",
    spot.SpotPrice, "instead of", spot.MaxPrice)

	// tag the spot instance request
  _, err = asg.awsManager.ec2.CreateTags(&ec2.CreateTagsInput{
		Resources: []*string{req.SpotInstanceRequestId},
		Tags: []*ec2.Tag{
			{
				Key:   aws.String("launched-for-asg"),
				Value: aws.String(asg.AwsRef.Name),
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("Can't tag spot instance request", err.Error())
	}

	return asg.waitForAndTagSpotInstance(req, group)
}

func getInstanceTags(
    group *autoscaling.Group) []*ec2.Tag {
	var tags []*ec2.Tag

	for _, asgTag := range group.Tags {
		if *asgTag.PropagateAtLaunch &&
        !strings.HasPrefix(*asgTag.Key, "aws:") {
			tags = append(tags, &ec2.Tag{
				Key:   asgTag.Key,
				Value: asgTag.Value,
			})
		}
	}
	return tags
}

func (asg *Asg) waitForAndTagSpotInstance(
    req *ec2.SpotInstanceRequest,
    group *autoscaling.Group) (*string, error) {
	glog.V(2).Infoln("Waiting for spot instance request",
		*req.SpotInstanceRequestId, "to be fulfilled")

	params := ec2.DescribeSpotInstanceRequestsInput{
		SpotInstanceRequestIds: []*string{req.SpotInstanceRequestId},
	}

	err := asg.awsManager.ec2.WaitUntilSpotInstanceRequestFulfilled(&params)
	if err != nil {
		return nil, fmt.Errorf("Error waiting for instance:", err.Error())
	}

	glog.V(2).Infoln("Done waiting for an instance.")

	// Now we try to get the InstanceID of the instance we got
	resp, err := asg.awsManager.ec2.DescribeSpotInstanceRequests(&params)
	if err != nil {
		return nil, fmt.Errorf("Failed to describe spot instance requests", err.Error())
	}

	// due to the waiter we can now safely assume all this data is available
	spotInstanceID := resp.SpotInstanceRequests[0].InstanceId

	glog.V(2).Infoln("Found new spot instance", *spotInstanceID,
    "- Tagging it to match the other instances from the group")

	tags := getInstanceTags(group)

  if len(tags) == 0 {
    return nil, nil
  }

  paramsTags := ec2.CreateTagsInput{
		Resources: []*string{spotInstanceID},
		Tags:      tags,
	}

  for n := 0; n < 60; n++ {
    if n > 0 {
      glog.V(2).Infoln("Sleeping for 5 seconds before retrying")
      time.Sleep(5 * time.Second)
    }
		_, err = asg.awsManager.ec2.CreateTags(&paramsTags)
		if err == nil {
			glog.V(2).Infoln("Instance", *spotInstanceID,
				"was tagged with the following tags:", tags)
			break
		}
		glog.Infoln(
      "Failed to create tags for the spot instance",
      *spotInstanceID, err.Error())
	}

  return spotInstanceID, err
}
