#include "utils_cm.hpp"

int main(int argc, char **argv){
    if(argc!=2){
        LOG(__LINE__, "wrong arg num");
        return -1;
    }
    // argv[1] = "12345";

    std::string_view port{argv[1]};
    RDMAServer server;
    server.listen("0.0.0.0", port);
    server.stop();
    return 0;
}