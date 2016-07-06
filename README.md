# ISKafkaBridge

First step is to register a producer .. 

First invoke createProducer Service .. It creates a static Producer object . You only need to pass your bootstrap servers .. . Rest everything can be defaulted .
 
    Invoke ::  ISKafkaIntegrationModule.service:createProducer 
    
        You can pass only bootstrapserver parameter ... The rest all can be defaulted ..
        
        
Then you should register a subscriber .. For this please 

    Invoke :: ISKafkaIntegrationModule.service:createConsumer
    
       It takes these inputs as parameters name
                bootstrapServers :: for ex : localhost:localhost:9092,localhost:9093,localhost:9094 .. This field is mandatory .
                name             ::  The name of your subscription . This field defintely needs to be passed .
                topicName  :: <Your topic > . This field is mandatory .
                clientGroupId :: <Your client group> : Fir ex: cg 
                enableAutoCommit :: "false" | "true" 
                autoCommitIntervalInMs :: <Some Integer in ms>
                sessionTimeOut :: <Some inteher in ms>
                serviceName :: <some service name> .. This service name gets invoked when the subscriber pulls a meesage from the Kafka topic.
                                                      This field is mandatory .
                
                
    Then you can send a message to a Kafka topic . For that please 
    
          Invoke :: ISKafkaIntegrationModule.service:sendData
          
            This service takes the following parameters 
                      topicName  :: The name of the topic .
                      keyName    :: The key name for the Kafka message
                      value      :: Kafka value in IData format .
                      callBackService  :: This service gets called asynchronously , when the message is sent to the Kafka servers .
