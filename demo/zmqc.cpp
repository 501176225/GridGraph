#include <zmq.hpp>
#include <unistd.h>
#include <iostream>

int main() {
    zmq::context_t context{1};
    zmq::socket_t socket{context, zmq::socket_type::req};
    socket.connect("tcp://localhost:5555");

    const std::string data{"Hello"};

    for (auto request_num = 0; request_num < 10; ++request_num) {
        // send the request message
        std::cout << "Sending Hello " << request_num << "..." << std::endl;
        socket.send(zmq::buffer(data), zmq::send_flags::none);
        
        // wait for reply from server
        zmq::message_t reply{};
        socket.recv(reply, zmq::recv_flags::none);

        std::cout << "Received " << reply.to_string(); 
        std::cout << " (" << request_num << ")";
        std::cout << std::endl;
    }
    return 0;
}