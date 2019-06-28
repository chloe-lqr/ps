#include "server/util/pending_buffer.hpp"

namespace csci5570 {

std::vector<Message> PendingBuffer::Pop(const int clock)
{
  std::vector<Message> poped_msg;
  if (buffer_.find(clock) != buffer_.end()) {
    poped_msg = std::move(buffer_[clock]);
    buffer_.erase(clock);
  }
  return poped_msg;
}

void PendingBuffer::Push(const int clock, Message& msg)
{
 buffer_[clock].push_back(std::move(msg)); 
}

int PendingBuffer::Size(const int progress) { return buffer_[progress].size(); }

}  // namespace csci5570
