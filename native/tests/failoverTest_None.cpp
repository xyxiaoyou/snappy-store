#include "Connection.h"
#include <iostream>
//#include <fstream>
#include <thread>
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
    //std::this_thread::sleep_for(std::chrono::seconds(300));
    conn.open("localhost", 1527,"app","app",properties);
    std::cout << "before stopping server- connected to :"<< conn.getCurrentHostAddress() <<std::endl;
    // creating dummy data
    conn.execute("drop table if exists FailOverTest.test");
    conn.execute("drop schema if exists  FailOverTest");
    conn.execute("create schema FailOverTest");
    conn.execute("create table FailOverTest.test(id int) as select id from range(120000)");
    auto count = conn.executeQuery("select * from FailOverTest.test");
    if(count > 0 )
      {
        std::cout << "Query execute successfully before server stop"<<std::endl;
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
    //stop the server
    std::thread t1(stopServer,stopRunningServer);
    t1.join();

    //std::this_thread::sleep_for(std::chrono::seconds(60));

    count = conn.executeQuery("select * from FailOverTest.test");
    
    std::cout << "Test executed successfully,connected to"<< conn.getCurrentHostAddress() <<std::endl;
    conn.execute("drop table if exists FailOverTest.test");
    conn.execute("drop schema if exists  FailOverTest");
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



