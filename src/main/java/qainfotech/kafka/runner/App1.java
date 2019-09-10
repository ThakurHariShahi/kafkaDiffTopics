package qainfotech.kafka.runner;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import org.apache.zookeeper.KeeperException;

import qait.kafka.stepDef.ConsumerCreator;
import qait.kafka.stepDef.ProducerCreator;
import qait.kafka.stepDef.kafkaConstants;

//TODO :- 1.TO REMOVE ERROR :- "Failed to load class "org.slf4j.impl.StaticLoggerBinder" 
//			USE DEPENDENCY "slf4j-simple"

public class App1 {
	public static void main(String[] args) throws KeeperException {
		App1 a  = new  App1();
		a.runProducer();
		a.runConsumer();
	}

	 void runConsumer() throws KeeperException {
		 ConsumerCreator c = new ConsumerCreator();
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>( c.createConsumer());
//		Consumer<Long, String> consumer = ConsumerCreator.createConsumer();
		try {
        consumer.subscribe(Collections.singletonList(kafkaConstants.PRODUCER_TOPIC_NAME));

		int noMessageFound = 0;
		while (true) {
			ConsumerRecords<String, String> consumerRecords = consumer.poll(300);// (1000);
			  
			// 1000 is the time in milliseconds consumer will wait if no record is found at
			// broker.
			if (consumerRecords.count() == 0) {
				
					System.out.println("###########" + noMessageFound);
					noMessageFound++;
				
				if (noMessageFound > kafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
					// If no message found count is reached to threshold exit loop.
					break;
				else
					continue;
			}System.out.println("123456789890");
			// print each record.
			consumerRecords.forEach(Arecord -> {
				System.out.println("Record Key " + Arecord.key());
				System.out.println("Record value " + Arecord.value());
				System.out.println("Record partition " + Arecord.partition());
				System.out.println("Record offset " + Arecord.offset());
			});
			// commits the offset of record to broker.
			consumer.commitAsync();
		}}
		catch(Exception e) {
			e.printStackTrace();
		}
		consumer.close();
	}

	 void runProducer() {
		 ProducerCreator p = new ProducerCreator(); 
		Producer<Long, String> producer = p.createProducer();

		for (int index = 0; index < kafkaConstants.MESSAGE_COUNT; index++) {
			ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(kafkaConstants.TOPIC_NAME,
					"This is record " + index);
			try {
				RecordMetadata metadata = producer.send(record).get();
				System.out.println("Record sent with key " + index + " to partition " + metadata.partition()
						+ " with offset " + metadata.offset());
			} catch (ExecutionException e) {
				System.out.println("Error in sending record");
				System.out.println(e);
			} catch (InterruptedException e) {
				System.out.println("Error in sending record");
				System.out.println(e);
			}
		}
	}
}
