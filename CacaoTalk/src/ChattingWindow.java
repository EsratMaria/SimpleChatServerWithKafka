import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
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
public class ChattingWindow {

	static ZkClient zkClient = null;
	static ZkUtils zkUtils = null;

	public static void chat_win(String iD) {

		String zookeeperHost = "163.239.22.22:2181";
		int sessionTimeOut = 15 * 1000;
		int connectionTimeOut = 10 * 1000;
		Properties props1 = new Properties();
		props1.put("zookeeperHost", "163.239.22.22:2181");
		props1.put("bootstrap.servers", "163.239.22.22:9092");

		zkClient = new ZkClient(zookeeperHost, sessionTimeOut, connectionTimeOut, ZKStringSerializer$.MODULE$);
		zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperHost), false);

		while (true) {
			System.out.println();
			System.out.println("Chatting Window");
			System.out.println("1. List");
			System.out.println("2. Make");
			System.out.println("3. Join");
			System.out.println("4. Log Out");

			Scanner sc = new Scanner(System.in);
			int user_choice = Integer.parseInt(sc.nextLine());

			switch (user_choice) {
			case 1:
				Map<String, List<PartitionInfo>> topicslist;
				Properties props = new Properties();
				props.put("bootstrap.servers", "163.239.22.22:9092");
				props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
				props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
				KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
				topicslist = consumer.listTopics();
				consumer.close();
				System.out.print(topicslist.keySet());

				break;
			case 2:
				System.out.println("Enter chat room name: ");
				String chat_room_name = sc.nextLine();
				String topicName = chat_room_name;
				int partitions = 1;
				int replication = 1;
				Properties topicConfiguration = new Properties();
				// topicConfiguration.put("cleanup.policy", "delete");

				// checking existence of similar topic
				if (!AdminUtils.topicExists(zkUtils, topicName)) {
					AdminUtils.createTopic(zkUtils, topicName, partitions, replication, topicConfiguration,
							RackAwareMode.Disabled$.MODULE$);

					System.out.println("\'" + chat_room_name + "\'" + " is created!");
				} else {
					System.out.println("\'" + chat_room_name + "\'" + " already exits! (duplication detecetd) ");
				}

				break;
			case 3:
				System.out.println("Enter the chat room name that you would like to join: ");
				String chat_room = sc.nextLine();
				System.out.println();
				// checking existence of requested chat room
				if (AdminUtils.topicExists(zkUtils, chat_room)) {
					ChatRoom.openChatRoom(iD, chat_room);
				} else {
					System.out.println("There is no chat room called: " + chat_room);
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

}
