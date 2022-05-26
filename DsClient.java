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
			// boolean variable to only get all servers once
			boolean getAllServers = true;
			// ArrayList allServers will contain all the servers
			List<String> allServers = new ArrayList<>();
			// Empty String to store the input stream
			String str = "";

			int jobID = 0, jobCores = 0, jobMem = 0, jobDisk = 0;

			dos.write(("HELO\n").getBytes());
			str = (String) in.readLine();
			//System.out.println(str);

			if (str.equals("OK")) {
				dos.write(("AUTH " + username + "\n").getBytes());
			}

			str = (String) in.readLine();
			//System.out.println(str);
			
			//while loop to keep running till the server send QUIT
			while (!(str.equals("QUIT"))) {
				dos.write(("REDY\n").getBytes());
				str = (String) in.readLine();

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
					
					// Only get all the server once when the client first runs
					if(getAllServers) {
						dos.write(("GETS All\n").getBytes());
						str = (String)in.readLine();
						int allServersCount = Integer.parseInt(str.split(" ")[1]);
						dos.write(("OK\n").getBytes());
						for(int i = 0; i < allServersCount; i++) {
							str = (String)in.readLine();
							allServers.add(str);
						}
						dos.write(("OK\n").getBytes());
						str = (String)in.readLine();
						// Setting getAllServers variable to false so that we only get all the servers once.
						getAllServers = false;
					}

					// bestServer contains the best possible server for the job
					String[] bestServer = getBestServer(jobCores, jobMem, jobDisk, str, dos, in).split(" ");

					// checking if the job type is JOBN or not so that jobs are only scheduled when needed
					if (job.split(" ")[0].equals("JOBN")) {
						dos.write(("SCHD " + jobID + " " + bestServer[0] + " " + bestServer[1] + "\n").getBytes());
						str = (String) in.readLine();
					}

					// Checking if the job type is JCPL and migrating waiting jobs to further optimize turnaround time
					if(job.split(" ")[0].equals("JCPL")){
						// waitingJobs string array list will contain all the current waiting jobs
						List<String> waitingJobs = new ArrayList<>();

						for(int i = 0; i < allServers.size(); i++) {
							dos.write(("LSTJ " + allServers.get(i).split(" ")[0] + " " + allServers.get(i).split(" ")[1] + "\n").getBytes());
							str = (String)in.readLine();
							// currJobs variable will contanin the number of current jobs on the server
							int currJobs = Integer.parseInt(str.split(" ")[1]);

							dos.write(("OK\n").getBytes());
							if(currJobs != 0){
								for(int j = 0; j < currJobs; j++) {
									str = (String)in.readLine();
									if(str.split(" ")[3].equals("-1")) {
										// adding all the waiting jobs to waitingJobs array list
										waitingJobs.add(str);
									}
								}
									dos.write(("OK\n").getBytes());
							}
							str = (String)in.readLine();
							for(int j = 0; j < waitingJobs.size(); j++) {
								// bestServer will contain the best server for the current job
                                bestServer = getBestServer(Integer.parseInt(waitingJobs.get(j).split(" ")[5]), Integer.parseInt(waitingJobs.get(j).split(" ")[6]), Integer.parseInt(waitingJobs.get(j).split(" ")[7]), str, dos, in).split(" ");
								dos.write(("MIGJ " + waitingJobs.get(j).split(" ")[0] + " " + allServers.get(i).split(" ")[0] + " " + allServers.get(i).split(" ")[1] + " " + bestServer[0] + " " + bestServer[1] + "\n").getBytes());
                                str = (String)in.readLine();
                            }
							// Clearing all the waiting jobs from the arraylist
                            waitingJobs.clear();
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
	
	// getBestServer function will return the best server for the given job
	public static String getBestServer(int jobCores, int jobMem, int jobDisk, String str, DataOutputStream dos, BufferedReader in){
        try {
			dos.write(("GETS Avail " + jobCores + " " + jobMem + " " + jobDisk + "\n").getBytes());
			str = (String)in.readLine();
			
			String[] servers = getData(str, dos, in);

			if(servers.length > 0) {
				dos.write(("OK\n").getBytes());
				str = (String)in.readLine();
			} else {
				str = (String)in.readLine();
			}
			
			// If no available servers are found, getting the server with least waiting time
			if(servers.length == 0) {
				dos.write(("GETS Capable "+ jobCores + " " + jobMem + " " + jobDisk + "\n").getBytes());
				str = (String) in.readLine();
		
				// servers string array contains data on all available servers
				servers = getData(str, dos, in);	

				dos.write(("OK\n").getBytes());
				str = (String)in.readLine();

				String[] serverWaitTime = new String[servers.length];
				for(int i = 0; i < servers.length; i++) {
					dos.write(("EJWT " + servers[i].split(" ")[0] + " " + servers[i].split(" ")[1] + "\n").getBytes());
					dos.flush();
					str = (String)in.readLine();
					serverWaitTime[i] = str;
				}

				// selecting the last server by default
				int selServer = servers.length-1;
				
				for(int i = 0; i < servers.length; i++) {
					String[] server = servers[i].split(" ");
					if(Integer.parseInt(server[4]) >= jobCores && Integer.parseInt(server[5]) >= jobMem && Integer.parseInt(server[6]) >= jobDisk){
						if(Integer.parseInt(serverWaitTime[selServer]) > Integer.parseInt(serverWaitTime[i])) {
							selServer = i;
						}
					}
				}
				// Sending the best server back to the caller
				return servers[selServer];
			}
			return servers[0];
        } catch(Exception e) {
            System.out.println(e);
        }
        return null;
	}

	public static String[] getData(String str, DataOutputStream dos, BufferedReader in){
		try{
			// serverCount varriable will contain the count of the servers
			int serverCount = Integer.parseInt(str.split(" ")[1]);
				dos.write(("OK\n").getBytes());
				// servers string array will contain the server data
				String[] servers =  new String[serverCount];
				for(int i = 0; i < servers.length; i++) {
					str = (String)in.readLine();
					servers[i] = str;
				}
			return servers;
		} catch (Exception e) {
			System.out.println(e);
		}
		return null;
	}
}
