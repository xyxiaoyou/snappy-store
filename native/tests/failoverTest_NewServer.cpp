#include "Connection.h"
#include <iostream>
#include <thread>
#include <string>
using namespace io::snappydata::client;
using namespace std;

void stopServer(string command){
  system(command.c_str());
}

int main(int argc, char **argv) {
  Connection conn;
  try {
    string snappyHomeDir(argv[1]);
    
    std::map<std::string, std::string> properties;
    properties.insert(std::pair<std::string, std::string>("load-balance","true"));
    
    conn.open("localhost", 1527,"app","app",properties);
    std::cout << "before stopping server connected to :"<< conn.getCurrentHostAddress() <<std::endl;

    //executing query in table already present-before stopping server
    auto count = conn.executeQuery("Show databases");
    
    if(count > 0 )
    {
      std::cout << "Query execute successfully stopping server"<<std::endl;
    }
    //create directory for a new server 2
    string createServerDir;
    createServerDir.append("cd  ").append(snappyHomeDir).append("; mkdir ./work/localhost-server-2");
    system(createServerDir.c_str());
    //start a server 2
    string serverStartScript;
    serverStartScript.append("cd  ").append(snappyHomeDir).append("; ./sbin/snappy-server.sh start -locators=localhost:10334 -dir=");
    string startNewServer ;
    startNewServer.append(serverStartScript).append("./work/localhost-server-2");
    system(startNewServer.c_str());
    //put on sleep
    std::this_thread::sleep_for(std::chrono::seconds(50));
    // stop the connected server 1
    string serverStopScript ;
    serverStopScript.append("cd  ").append(snappyHomeDir).append("; ./sbin/snappy-server.sh stop -dir=");
    string stopRunningServer;
    stopRunningServer.append(serverStopScript).append("./work/localhost-server-1");
    std::thread t1(stopServer,stopRunningServer);
    t1.join();

    //executing query in table already present-after stopping server 
    count =  conn.executeQuery("Show databases");
    if(count > 0 )
    {
      std::cout << "Query execute successfully after server stop"<<std::endl;
    }
    std::cout << "Test executed successfully connected to :"<< conn.getCurrentHostAddress() <<std::endl;
    conn.close();
    //stop server 2
    serverStopScript.append("./work/localhost-server-2");
    system(serverStopScript.c_str());
    //start the server 1 again
    serverStartScript.append("./work/localhost-server-1");
    system(serverStartScript.c_str());
    //create directory for a new server 2
    string removeServerDir;
    removeServerDir.append("cd  ").append(snappyHomeDir).append("; rm -r ./work/localhost-server-2");
    system(removeServerDir.c_str());
    } catch (SQLException& sqle) {
        if(conn.isOpen()) conn.close();
        std::cout<< "ExecuteQuery failed, throws exception"<<std::endl;
        sqle.printStackTrace(std::cout);
    }
}



