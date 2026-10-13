#pragma once

#include <map>
#include <memory>
#include <string>

#include "envoy/common/platform.h"
#include "envoy/network/filter.h"

#include "source/common/buffer/buffer_impl.h"
#include "source/common/common/logger.h"
#include "source/common/protobuf/utility.h"

#include "contrib/rocketmq_proxy/filters/network/source/protocol.h"

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace RocketmqProxy {

enum MessageVersion : uint32_t {
  V1 = (0xAABBCCDDU ^ 1880681586U) + 8U,
  V2 = (0xAABBCCDDU ^ 1880681586U) + 4U
};

class Decoder : Logger::Loggable<Logger::Id::rocketmq> {
public:
  Decoder() = default;

  ~Decoder() = default;

  /**
   * @param buffer Data buffer to decode.
   * @param underflow Indicate if buffer contains enough data in terms of protocol frame.
   * @param has_error Indicate if the decoding is successful or not.
   * @param request_code Corresponding request code if applies.
   * @return Decoded remote command.
   */
  static RemotingCommandPtr decode(Buffer::Instance& buffer, bool& underflow, bool& has_error,
                                   int request_code = 0);

  static std::string decodeTopic(Buffer::Instance& buffer, int32_t cursor);

  static int32_t decodeQueueId(Buffer::Instance& buffer, int32_t cursor);

  static int64_t decodeQueueOffset(Buffer::Instance& buffer, int32_t cursor);

  static std::string decodeMsgId(Buffer::Instance& buffer, int32_t cursor);

  static constexpr uint32_t MIN_FRAME_SIZE = 8;

  static constexpr uint32_t MAX_FRAME_SIZE = 4 * 1024 * 1024;

  static constexpr uint32_t FRAME_LENGTH_FIELD_SIZE = 4;

  static constexpr uint32_t FRAME_HEADER_LENGTH_FIELD_SIZE = 4;

private:
  static uint32_t adjustHeaderLength(uint32_t len) { return len & 0xFFFFFFu; }

  static bool isJsonHeader(uint32_t len) { return (len >> 24u) == 0; }

  /**
   * Decode the extended header of a request, if the request code defines one.
   * @param custom_header Set to the decoded header, or nullptr if there is none to decode.
   * @return false if the extended header is missing or malformed.
   */
  static bool decodeExtHeader(RequestCode code, Protobuf::Struct& header_struct,
                              CommandCustomHeaderPtr& custom_header);

  /**
   * Decode the extended header of a response, if the response to the request code defines one.
   * @param custom_header Set to the decoded header, or nullptr if there is none to decode.
   * @return false if the extended header is missing or malformed.
   */
  static bool decodeResponseExtHeader(ResponseCode response_code, Protobuf::Struct& header_struct,
                                      RequestCode request_code,
                                      CommandCustomHeaderPtr& custom_header);

  static bool decodeCustomHeader(std::unique_ptr<CommandCustomHeader> header,
                                 Protobuf::Struct& header_struct,
                                 CommandCustomHeaderPtr& custom_header);

  static bool isComplete(Buffer::Instance& buffer, int32_t cursor);

  // Returns true if the buffer holds at least `size` bytes starting at `offset`.
  static bool hasBytes(const Buffer::Instance& buffer, int64_t offset, uint64_t size) {
    return offset >= 0 && static_cast<uint64_t>(offset) <= buffer.length() &&
           size <= buffer.length() - offset;
  }
};

class Encoder {
public:
  static void encode(const RemotingCommandPtr& command, Buffer::Instance& buffer);
};

} // namespace RocketmqProxy
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy
