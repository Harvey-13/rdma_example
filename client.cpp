#include "utils_cm.hpp"


int main(int argc, char** argv){
    if(argc != 4){
        LOG(__LINE__, "wrong arg num");
        return -1;
    }
    // argv[1] = "192.168.200.53";
    // argv[2] = "12345", argv[3] = "hello";

    std::string_view port{argv[2]};
    RDMAClient client;
    client.connect(argv[1], port);
    client.post_send(argv[3]);
    LOG(__LINE__, "client send:", argv[3]);
    LOG(__LINE__, "client recv:", client.post_recv());
    client.close();
    return 0;
}