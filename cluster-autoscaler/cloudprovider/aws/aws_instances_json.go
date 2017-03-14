package aws

import (
  "fmt"
  "os"
  "encoding/json"
  "io"
  "io/ioutil"
  "net/http"
  "time"

	"github.com/golang/glog"
)

const (
	jsonInstancesCacheExpire = 24 * time.Hour
)

type jsonInstance struct {
	Family             string                  `json:"family"`
	EnhancedNetworking bool                    `json:"enhanced_networking"`
	VCPU               int                     `json:"vCPU"`
	Generation         string                  `json:"generation"`
	EBSIOPS            float32                 `json:"ebs_iops"`
	NetworkPerformance string                  `json:"network_performance"`
	EBSThroughput      float32                 `json:"ebs_throughput"`
	PrettyName         string                  `json:"pretty_name"`
	Pricing            map[string]regionPrices `json:"pricing"`

	Storage *storageConfiguration `json:"storage"`

	VPC struct {
		//    IPsPerENI int `json:"ips_per_eni"`
		//    MaxENIs   int `json:"max_enis"`
	} `json:"vpc"`

	Arch                     []string `json:"arch"`
	LinuxVirtualizationTypes []string `json:"linux_virtualization_types"`
	EBSOptimized             bool     `json:"ebs_optimized"`

	MaxBandwidth float32 `json:"max_bandwidth"`
	InstanceType string  `json:"instance_type"`

	// ECU float32 `json:"ECU"`

	Memory          float32 `json:"memory"`
	EBSMaxBandwidth float32 `json:"ebs_max_bandwidth"`
}

type storageConfiguration struct {
	SSD     bool    `json:"ssd"`
	Devices int     `json:"devices"`
	Size    float32 `json:"size"`
}

type regionPrices struct {
	Linux struct {
		OnDemand string `json:"ondemand"`
	} `json:"linux"`
}
type InstanceData []jsonInstance
type InstanceDataMap map[string]jsonInstance

func getFile(filepath string, url string) (err error) {
  // Create the file
  out, err := os.Create(filepath)
  if err != nil  {
    return err
  }
  defer out.Close()

  // Get the data
  resp, err := http.Get(url)
  if err != nil {
    return err
  }
  defer resp.Body.Close()

  // Writer the body to file
  _, err = io.Copy(out, resp.Body)
  if err != nil  {
    return err
  }

  return nil
}

func loadJSONInstancesData(filepath string) (InstanceData, error) {
  // Get the file information
  stat, err := os.Stat(filepath)

  // Fail only if the file _exists_ and we were not able to get the information
  if err != nil && ! os.IsNotExist(err) {
    return nil, fmt.Errorf("File error when using 'stat': %v\n", err)
  }

  // Check if the file is up to date or if a new version in required
  if os.IsNotExist(err) || stat.ModTime().Before(time.Now().Add(-jsonInstancesCacheExpire)) {
    url := "http://www.ec2instances.info/instances.json"
    glog.Infoln("Downloading instances information from", url)
    getFile(filepath, url)
  }

  // Reding the file content to load the instances information
  file, err := ioutil.ReadFile(filepath)
  if err != nil {
    return nil, fmt.Errorf("File error when using 'ReadFile': %v\n", err)
  }

  // Unmarshal the JSON to a local instance object
  var instanceData InstanceData
  err = json.Unmarshal(file, &instanceData)
  if err != nil {
    return nil, fmt.Errorf("JSON error when using 'Unmarshal': %v\n", err)
  }

  // Returned the instances data
  return instanceData, nil
}

func loadJSONInstancesDataAsMap(filepath string) (InstanceDataMap, error) {
  // Load the instances data normally
  instancesData, err := loadJSONInstancesData(filepath)
  if err != nil {
    return nil, err
  }

  // Convert them to a map from the type of instance to the information
  // about that kind of instance
  instancesMap := make(InstanceDataMap)
  for _, instanceData := range instancesData {
    instancesMap[instanceData.InstanceType] = instanceData
  }

  return instancesMap, nil
}
