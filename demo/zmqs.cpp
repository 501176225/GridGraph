#include <zmq.hpp>
#include <unistd.h>
#include <iostream>

int main() {
    zmq::context_t context{1};

    zmq::socket_t socket{context, zmq::socket_type::rep};
    socket.bind("tcp://*:5555");
    const std::string data{"World"};
    int i = 0;

    while (true) {
        std::cout<<i++<<std::endl;
        zmq::message_t request;
        socket.recv(request, zmq::recv_flags::none);
        std::cout << "Received " << request.to_string() << std::endl;

        sleep(1);

        socket.send(zmq::buffer(data), zmq::send_flags::none);
    }    
    return 0;
}

// g++ -O3 -Wall -std=c++11 -g zmqs.cpp -lzmq
