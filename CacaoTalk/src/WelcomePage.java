import java.util.Scanner;

/**
 * 
 */

/**
 * @author Esrat Maria
 *
 */
public class WelcomePage {

	/**
	 * @param args
	 */
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("Welcome to CacalTalk");
		while (true) {
			System.out.println("1. Log in");
			System.out.println("2. Exit");

			Scanner sc = new Scanner(System.in);
			int user_choice = Integer.parseInt(sc.nextLine());

			switch (user_choice) {
			case 1:
				System.out.println("Enter your ID: ");
				String ID = sc.nextLine();
				System.out.println("cacaotalk> ID: " + ID);
				System.out.println("Setting up connection.. Please wait!");
				System.out.println();
				ChattingWindow.chat_win(ID);

				break;

			case 2:
				System.exit(0);

				break;

			default:
				System.out.println("Invalid input! Choose either 1 or 2");
			}
		}

	}

}
