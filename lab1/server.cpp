#include <iostream>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

int main() {
    int sd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sd < 0) {
        perror("socket");
        return 1;
    }

    sockaddr_in saddr{};
    saddr.sin_family = AF_INET;
    saddr.sin_port = htons(8080);
    saddr.sin_addr.s_addr = INADDR_ANY;

    if (bind(sd, (sockaddr*)&saddr, sizeof(saddr)) < 0) {
        perror("bind");
        close(sd);
        return 1;
    }

    std::cout << "Echo test test server running on port 8080" << std::endl;

    char buf[512];
    sockaddr_in caddr{};
    socklen_t clen;

    while (true) {
        clen = sizeof(caddr);
        int n = recvfrom(sd, buf, sizeof(buf) - 1, 0, (sockaddr*)&caddr, &clen);
        if (n <= 0)
            continue;

        buf[n] = '\0';
        std::cout << "[" << inet_ntoa(caddr.sin_addr) << ":" << ntohs(caddr.sin_port) << "] " << buf << std::endl;

        sendto(sd, buf, n, 0, (sockaddr*)&caddr, clen);
    }

    close(sd);
    return 0;
}
