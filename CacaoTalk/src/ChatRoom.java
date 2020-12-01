import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Time;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZkUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;

/**
 * 
 */

/**
 * @author Esrat Maria
 *
 */
public class ChatRoom {

	public static void openChatRoom(String iD, String chat_room_name) {
		// TODO Auto-generated method stub
		System.out.println();
		System.out.println("1. Read");
		System.out.println("2. Write");
		System.out.println("3. Reset");
		System.out.println("4. Exit");

		Scanner sc = new Scanner(System.in);
		int user_choice = Integer.parseInt(sc.nextLine());

		switch (user_choice) {
		case 1:
			String bootstrapServer = "163.239.22.22:9092";
			String keyDeserializer = StringDeserializer.class.getName();
			String valueDeserializer = StringDeserializer.class.getName();

			String groupID = "consumerGroup1";
			String offsetReset = "earliest";

			String topicName = chat_room_name;

			Properties properties = new Properties();

			properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
			properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
			properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
			properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupID);
			properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset);

			KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
			kafkaConsumer.subscribe(Arrays.asList(topicName));
			
			ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(1000);
			// System.out.println(consumerRecords.count());
			for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {

				System.out.println("Text> " + consumerRecord.value());
			}
			break;

		case 2:
			Properties props = new Properties();
			props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "163.239.22.22:9092");
			props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
			props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

			KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
			String user_record = sc.nextLine();

			ProducerRecord<String, String> record = new ProducerRecord<>(chat_room_name, null, user_record);

			producer.send(record);
			producer.flush();
			System.out.println("record inserted!");

			break;
		case 3:
			Properties property = new Properties();
			property.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "163.239.22.22:9092");
	        property.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer");
	        property.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
	        property.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
	        property.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
	        property.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
	        property.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

	        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(property);
	        consumer.subscribe(Collections.singletonList(chat_room_name));
	        consumer.seekToBeginning(consumer.assignment());
	        ConsumerRecords<Integer, String> records = consumer.poll(1000);
	        for (ConsumerRecord<Integer, String> rec : records) {
	            System.out.println("Received message: (" + rec.topic().getClass() + ", " + rec.value() + ") at offset " + rec.offset());
	        }

			break;

		case 4:
			System.exit(0);

			break;

		default:
			System.out.println("Invalid input! Choose from 1-4");
		}

	}

}
