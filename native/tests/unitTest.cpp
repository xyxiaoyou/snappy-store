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
  try {
    string snappyHomeDir(argv[1]);
    string serverStopScript ;
    serverStopScript.append("cd  ").append(snappyHomeDir).append("; ./sbin/snappy-server.sh stop -dir=");
    std::map<std::string, std::string> properties;
    properties.insert(std::pair<std::string, std::string>("load-balance","true"));
    Connection conn;
    conn.open("localhost", 1527,"app","app",properties);

    int connectPort = conn.getCurrentHostAddress().port;
    string serverDir="./work/";
    if(connectPort > 1528){
      serverDir.append("localhost-server-2");
    }
    serverDir.append("localhost-server-1");
    serverStopScript.append(serverDir);

    for(int i=0;i <10;i++)
    {
      conn.executeQuery("Show databases");
    }
    std::cout << "before thread connected to :"<< conn.getCurrentHostAddress() <<std::endl;

    std::thread t1(stopServer,serverStopScript);
    t1.join();
    for(int i=0;i <10;i++)
    {
      conn.executeQuery("Show databases");
    }
    std::cout << "after thread connected to :"<< conn.getCurrentHostAddress() <<std::endl;
    } catch (SQLException& sqle) {
        sqle.printStackTrace(std::cout);
    }
}



