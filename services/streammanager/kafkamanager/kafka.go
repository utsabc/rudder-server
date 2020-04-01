package Kafkamanager

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

/* type KafkaManager struct {
	Config   KafkaConfig
	Producer *kafka.Producer
} */

type KafkaManager struct {
	Config KafkaConfig
}

type KafkaConfig struct {
	Topic   string
	Host    string
	ClintId string
}

type StreamOutput struct {
	Message string
	Error   error
}

/* func New(Config map[string]interface{}) (KafkaManager, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"client.id":         "test-kafka-prabrisha"})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		panic(err)
	}

	fmt.Printf("Created Producer %v\n", p)

	config := KafkaConfig{Topic: "myTestTopic"}
	return KafkaManager{Config: config, Producer: p}, nil

} */

func New(Config map[string]interface{}) (KafkaManager, error) {
	topic := Config["topic"].(string)
	hostName := Config["hostName"].(string)
	config := KafkaConfig{Topic: topic, Host: hostName, ClintId: "kafka-rudder"}
	//config := KafkaConfig{Topic: "myTestTopic", Host: "localhost:9092", ClintId: "test-kafka-prabrisha"}
	return KafkaManager{Config: config}, nil

}

func (manager *KafkaManager) Produce(file *os.File) (string, error) {
	fmt.Println("in Kafka produce")

	// producer := manager.Producer

	config := manager.Config

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": config.Host,
		"client.id":         config.ClintId})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		panic(err)
	}

	fmt.Printf("Created Producer %v\n", producer)

	topic := config.Topic

	produceChan := make(chan StreamOutput)

	go func() {
		defer close(produceChan)
		var returnValue StreamOutput
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				m := ev

				if m.TopicPartition.Error != nil {
					message := fmt.Sprintf("Delivery failed: %v\n", m.TopicPartition.Error)
					returnValue = StreamOutput{Message: message, Error: m.TopicPartition.Error}
				} else {
					message := fmt.Sprintf("Delivered message to topic %s [%d] at offset %v\n",
						*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
					returnValue = StreamOutput{Message: message, Error: nil}
				}
				produceChan <- returnValue
				return

			default:
				returnValue = StreamOutput{Message: "Ignored event", Error: fmt.Errorf("ignored event: %s", ev)}
				produceChan <- returnValue
				return
			}
		}
	}()

	value := getDataFromFile(file)
	producer.ProduceChannel() <- &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny}, Value: []byte(value)}

	// wait for delivery report goroutine to finish
	result := <-produceChan
	fmt.Println(result)
	producer.Close()

	return result.Message, result.Error
}

func getDataFromFile(file *os.File) []byte {
	reader, _ := gzip.NewReader(file)
	sc := bufio.NewScanner(reader)
	lineBytesCounter := 0
	content := make([]byte, 0)
	for sc.Scan() {
		lineBytes := sc.Bytes()
		lineBytesCounter += len(lineBytes)
		content = append(content, lineBytes...)
		content = append(content, "\n"...)
	}
	file.Close()
	// s := string(content)
	// fmt.Println(s)
	return content
}
