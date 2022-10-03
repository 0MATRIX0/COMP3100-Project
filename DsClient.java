// Importing necessary packages
import java.io.*;
import java.net.*;
import java.util.*;

public class DsClient {
	public static void main(String[] args) {
		try {
			// Creating a socket
			Socket s = new Socket("localhost", 50000);
			// Initialising data output stream associated with the socket
			DataOutputStream dos = new DataOutputStream(s.getOutputStream());
			// Initialising data input stream associated with the socket
			BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));
			// Creating a variable username to store the username of the system
			String username = System.getProperty("user.name");
			// Empty string to store the input stream
			String str = "";
			// String arraylist to store dynamically store the big servers (saves memory)
			List<String> bigServers = new ArrayList<>();
			// Variable to store the current position of the server instance
			int currentServer = 0;
			// boolean variable used to only store the servers once
			boolean getServers = true;

			dos.write(("HELO\n").getBytes());
			str = (String) in.readLine();
			System.out.println(str);

			if (str.equals("OK")) {
				dos.write(("AUTH " + username + "\n").getBytes());
			}

			str = (String) in.readLine();
			System.out.println(str);
			
			//while loop to keep running till the server send QUIT
			while (!(str.equals("QUIT"))) {
				dos.write(("REDY\n").getBytes());
				str = (String) in.readLine();
				System.out.println(str);

				if (!(str.equals("NONE"))) {
					// job variable will store the current job
					String job = str;
					// jobID variable will store the ID of the current job
					int jobID = Integer.parseInt(str.split(" ")[2]);

					dos.write(("GETS All\n").getBytes());
					str = (String) in.readLine();
					System.out.println(str);	

					// serverCount variable contains the total count of the servers available. This will be used to create a String array of specific size.
					int serverCount = Integer.parseInt(str.split(" ")[1]);
					dos.write(("OK\n").getBytes());
					
					// servers string array contains data on all available servers
					String[] servers = new String[serverCount];	
					
					// sending data to servers variable
					for (int i = 0; i < serverCount; i++) {
						str = (String) in.readLine();
						System.out.println(str);
						servers[i] = str;
					}

					// This if condition will only run once after the program is run. 
					if(getServers){
						// maxCores variable will store the maximum numbers of cores available
						int maxCores = 0;
						// serverName variable will store the name of the server will maximum cores
						String serverName = "";

						// getting the max cores value and server name
						for (int i = 0; i<servers.length; i++) {
							if(Integer.parseInt(servers[i].split(" ")[4]) > maxCores) {
								maxCores = Integer.parseInt(servers[i].split(" ")[4]);
								serverName = servers[i].split(" ")[0];
							}
						}

						// adding the server to bidServers arraylist if its cores are same as maxCores
						for(int i = 0; i<servers.length; i++) {
							if(servers[i].split(" ")[0].equals(serverName) && Integer.parseInt(servers[i].split(" ")[4]) == maxCores) {
								bigServers.add(servers[i]);
							}
						}

						//setting getServers to false so that the if conditional block will only run once
						getServers = false;
					}
					
					// bigServer string array will contain the data of a single big server
					String[] bigServer = bigServers.get(currentServer).split(" ");
					
					dos.write(("OK\n").getBytes());
					str = (String) in.readLine();
					System.out.println(str);

					// checking if the job type is JOBN or not so that jobs are only scheduled when needed
					if (str.equals(".") && job.split(" ")[0].equals("JOBN")) {
						// adding 1 to current server so that the next big server will be used
						currentServer++;

						dos.write(("SCHD " + jobID + " " + bigServer[0] + " " + bigServer[1] + "\n").getBytes());
						str = (String) in.readLine();
						System.out.println(str);

						// once all the big servers are used, currentServer will reset back to 0 to schedule jobs from the first server again
						if(currentServer == bigServers.size()) {
							currentServer = 0;
						}
					}
				} else {
					dos.write("QUIT\n".getBytes());
					str = (String) in.readLine();
					System.out.println(str);
				}
			}
			// flushing output stream
			dos.flush();
			// closing output stream
			dos.close();
			// closing socket connection
			s.close();
		} catch (Exception e) {
			// printing error to console if there is any
			System.out.println(e);
		}
	}
}
