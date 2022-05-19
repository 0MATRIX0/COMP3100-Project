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

			int jobID = 0, jobCores = 0, jobMem = 0, jobDisk = 0;

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
					if(job.split(" ")[0].equals("JOBN")){
						// jobID variable will store the ID of the current job
						jobID = Integer.parseInt(str.split(" ")[2]);
						// jobCores variable will store the cored required for the job
						jobCores = Integer.parseInt(str.split(" ")[4]);
						// jobMem variable will store the memory required for the current job
						jobMem = Integer.parseInt(str.split(" ")[5]);
						// jobDisk variable will store the disk space required for the current job
						jobDisk = Integer.parseInt(str.split(" ")[6]);
					}
					System.out.println(jobID + "  " + jobCores + "  " + jobMem + "  " + jobDisk);
					
					dos.write(("GETS Capable "+jobCores+" "+jobMem+" "+jobDisk+"\n").getBytes());
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
						servers[i] = str;
					}
					
					// capableServer string array will contain the data of a single capable server
					String[] capableServer = servers[0].split(" ");
					for(int i = 0; i < servers.length; i++) {
						String[] server = servers[i].split(" ");
						if(Integer.parseInt(server[4]) >= jobCores && Integer.parseInt(server[5]) >= jobMem && Integer.parseInt(server[6]) >= jobDisk){
							capableServer = servers[i].split(" ");
						       	break;	
						}	
					}

					for(int i= 0; i<capableServer.length; i++) {
						System.out.print(capableServer[i] + " ");
					}
					System.out.println("\n");

					dos.write(("OK\n").getBytes());
					str = (String) in.readLine();
					System.out.println(str);

					// checking if the job type is JOBN or not so that jobs are only scheduled when needed
					if (str.equals(".") && job.split(" ")[0].equals("JOBN")) {
						dos.write(("SCHD " + jobID + " " + capableServer[0] + " " + capableServer[1] + "\n").getBytes());
						str = (String) in.readLine();
						System.out.println(str);
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
