#include "contrib/rocketmq_proxy/filters/network/source/protocol.h"

#include "source/common/common/assert.h"
#include "source/common/common/enum_to_int.h"

#include "absl/strings/numbers.h"
#include "contrib/rocketmq_proxy/filters/network/source/constant.h"

namespace Envoy {
namespace Extensions {
namespace NetworkFilters {
namespace RocketmqProxy {

namespace {

using FieldMap = Protobuf::Map<std::string, Protobuf::Value>;

// Reads a required string field. Returns false if the field is missing.
bool readString(const FieldMap& members, const std::string& key, std::string& value) {
  const auto it = members.find(key);
  if (it == members.end()) {
    return false;
  }
  value = it->second.string_value();
  return true;
}

// Reads a required integer field, which may be serialized either as a JSON number or as a string.
// Returns false if the field is missing or is a string that is not a valid integer of type T.
template <typename T> bool readInt(const FieldMap& members, const std::string& key, T& value) {
  const auto it = members.find(key);
  if (it == members.end()) {
    return false;
  }
  if (it->second.kind_case() == Protobuf::Value::kNumberValue) {
    value = static_cast<T>(it->second.number_value());
    return true;
  }
  return absl::SimpleAtoi(it->second.string_value(), &value);
}

} // namespace

void SendMessageRequestHeader::encode(Protobuf::Value& root) {
  auto& members = *(root.mutable_struct_value()->mutable_fields());

  switch (version_) {
  case SendMessageRequestVersion::V1: {
    Protobuf::Value producer_group_v;
    producer_group_v.set_string_value(producer_group_);
    members["producerGroup"] = producer_group_v;

    Protobuf::Value topic_v;
    topic_v.set_string_value(topic_.c_str(), topic_.length());
    members["topic"] = topic_v;

    Protobuf::Value default_topic_v;
    default_topic_v.set_string_value(default_topic_);
    members["defaultTopic"] = default_topic_v;

    Protobuf::Value default_topic_queue_number_v;
    default_topic_queue_number_v.set_number_value(default_topic_queue_number_);
    members["defaultTopicQueueNums"] = default_topic_queue_number_v;

    Protobuf::Value queue_id_v;
    queue_id_v.set_number_value(queue_id_);
    members["queueId"] = queue_id_v;

    Protobuf::Value sys_flag_v;
    sys_flag_v.set_number_value(sys_flag_);
    members["sysFlag"] = sys_flag_v;

    Protobuf::Value born_timestamp_v;
    born_timestamp_v.set_number_value(born_timestamp_);
    members["bornTimestamp"] = born_timestamp_v;

    Protobuf::Value flag_v;
    flag_v.set_number_value(flag_);
    members["flag"] = flag_v;

    if (!properties_.empty()) {
      Protobuf::Value properties_v;
      properties_v.set_string_value(properties_.c_str(), properties_.length());
      members["properties"] = properties_v;
    }

    if (reconsume_time_ > 0) {
      Protobuf::Value reconsume_times_v;
      reconsume_times_v.set_number_value(reconsume_time_);
      members["reconsumeTimes"] = reconsume_times_v;
    }

    if (unit_mode_) {
      Protobuf::Value unit_mode_v;
      unit_mode_v.set_bool_value(unit_mode_);
      members["unitMode"] = unit_mode_v;
    }

    if (batch_) {
      Protobuf::Value batch_v;
      batch_v.set_bool_value(batch_);
      members["batch"] = batch_v;
    }

    if (max_reconsume_time_ > 0) {
      Protobuf::Value max_reconsume_time_v;
      max_reconsume_time_v.set_number_value(max_reconsume_time_);
      members["maxReconsumeTimes"] = max_reconsume_time_v;
    }
    break;
  }
  case SendMessageRequestVersion::V2: {
    Protobuf::Value producer_group_v;
    producer_group_v.set_string_value(producer_group_.c_str(), producer_group_.length());
    members["a"] = producer_group_v;

    Protobuf::Value topic_v;
    topic_v.set_string_value(topic_.c_str(), topic_.length());
    members["b"] = topic_v;

    Protobuf::Value default_topic_v;
    default_topic_v.set_string_value(default_topic_.c_str(), default_topic_.length());
    members["c"] = default_topic_v;

    Protobuf::Value default_topic_queue_number_v;
    default_topic_queue_number_v.set_number_value(default_topic_queue_number_);
    members["d"] = default_topic_queue_number_v;

    Protobuf::Value queue_id_v;
    queue_id_v.set_number_value(queue_id_);
    members["e"] = queue_id_v;

    Protobuf::Value sys_flag_v;
    sys_flag_v.set_number_value(sys_flag_);
    members["f"] = sys_flag_v;

    Protobuf::Value born_timestamp_v;
    born_timestamp_v.set_number_value(born_timestamp_);
    members["g"] = born_timestamp_v;

    Protobuf::Value flag_v;
    flag_v.set_number_value(flag_);
    members["h"] = flag_v;

    if (!properties_.empty()) {
      Protobuf::Value properties_v;
      properties_v.set_string_value(properties_.c_str(), properties_.length());
      members["i"] = properties_v;
    }

    if (reconsume_time_ > 0) {
      Protobuf::Value reconsume_times_v;
      reconsume_times_v.set_number_value(reconsume_time_);
      members["j"] = reconsume_times_v;
    }

    if (unit_mode_) {
      Protobuf::Value unit_mode_v;
      unit_mode_v.set_bool_value(unit_mode_);
      members["k"] = unit_mode_v;
    }

    if (batch_) {
      Protobuf::Value batch_v;
      batch_v.set_bool_value(batch_);
      members["m"] = batch_v;
    }

    if (max_reconsume_time_ > 0) {
      Protobuf::Value max_reconsume_time_v;
      max_reconsume_time_v.set_number_value(max_reconsume_time_);
      members["l"] = max_reconsume_time_v;
    }
    break;
  }
  default:
    break;
  }
}

bool SendMessageRequestHeader::decode(const Protobuf::Value& ext_fields) {
  const auto& members = ext_fields.struct_value().fields();
  switch (version_) {
  case SendMessageRequestVersion::V1: {
    if (!readString(members, "producerGroup", producer_group_) ||
        !readString(members, "topic", topic_) ||
        !readString(members, "defaultTopic", default_topic_) ||
        !readInt(members, "defaultTopicQueueNums", default_topic_queue_number_) ||
        !readInt(members, "queueId", queue_id_) || !readInt(members, "sysFlag", sys_flag_) ||
        !readInt(members, "bornTimestamp", born_timestamp_) || !readInt(members, "flag", flag_)) {
      return false;
    }

    if (members.contains("properties")) {
      properties_ = members.at("properties").string_value();
    }

    if (members.contains("reconsumeTimes") &&
        !readInt(members, "reconsumeTimes", reconsume_time_)) {
      return false;
    }

    if (members.contains("unitMode")) {
      if (members.at("unitMode").kind_case() == Protobuf::Value::kBoolValue) {
        unit_mode_ = members.at("unitMode").bool_value();
      } else {
        unit_mode_ = (members.at("unitMode").string_value() == std::string("true"));
      }
    }

    if (members.contains("batch")) {
      if (members.at("batch").kind_case() == Protobuf::Value::kBoolValue) {
        batch_ = members.at("batch").bool_value();
      } else {
        batch_ = (members.at("batch").string_value() == std::string("true"));
      }
    }

    if (members.contains("maxReconsumeTimes") &&
        !readInt(members, "maxReconsumeTimes", max_reconsume_time_)) {
      return false;
    }
    break;
  }

  case SendMessageRequestVersion::V2: {
    if (!readString(members, "a", producer_group_) || !readString(members, "b", topic_) ||
        !readString(members, "c", default_topic_) ||
        !readInt(members, "d", default_topic_queue_number_) || !readInt(members, "e", queue_id_) ||
        !readInt(members, "f", sys_flag_) || !readInt(members, "g", born_timestamp_) ||
        !readInt(members, "h", flag_)) {
      return false;
    }

    if (members.contains("i")) {
      properties_ = members.at("i").string_value();
    }

    if (members.contains("j") && !readInt(members, "j", reconsume_time_)) {
      return false;
    }

    if (members.contains("k")) {
      if (members.at("k").kind_case() == Protobuf::Value::kBoolValue) {
        unit_mode_ = members.at("k").bool_value();
      } else {
        unit_mode_ = (members.at("k").string_value() == std::string("true"));
      }
    }

    if (members.contains("m")) {
      if (members.at("m").kind_case() == Protobuf::Value::kBoolValue) {
        batch_ = members.at("m").bool_value();
      } else {
        batch_ = (members.at("m").string_value() == std::string("true"));
      }
    }

    if (members.contains("l") && !readInt(members, "l", max_reconsume_time_)) {
      return false;
    }
    break;
  }
  default:
    ENVOY_LOG(error, "Unknown SendMessageRequestVersion: {}", static_cast<int>(version_));
    return false;
  }
  return true;
}

void SendMessageResponseHeader::encode(Protobuf::Value& root) {
  auto& members = *(root.mutable_struct_value()->mutable_fields());

  ASSERT(!msg_id_.empty());
  Protobuf::Value msg_id_v;
  msg_id_v.set_string_value(msg_id_.c_str(), msg_id_.length());
  members["msgId"] = msg_id_v;

  ASSERT(queue_id_ >= 0);
  Protobuf::Value queue_id_v;
  queue_id_v.set_number_value(queue_id_);
  members["queueId"] = queue_id_v;

  ASSERT(queue_offset_ >= 0);
  Protobuf::Value queue_offset_v;
  queue_offset_v.set_number_value(queue_offset_);
  members["queueOffset"] = queue_offset_v;

  if (!transaction_id_.empty()) {
    Protobuf::Value transaction_id_v;
    transaction_id_v.set_string_value(transaction_id_.c_str(), transaction_id_.length());
    members["transactionId"] = transaction_id_v;
  }
}

bool SendMessageResponseHeader::decode(const Protobuf::Value& ext_fields) {
  const auto& members = ext_fields.struct_value().fields();
  if (!readString(members, "msgId", msg_id_) || !readInt(members, "queueId", queue_id_) ||
      !readInt(members, "queueOffset", queue_offset_)) {
    return false;
  }

  if (members.contains("transactionId")) {
    transaction_id_ = members.at("transactionId").string_value();
  }
  return true;
}

void GetRouteInfoRequestHeader::encode(Protobuf::Value& root) {
  auto& members = *(root.mutable_struct_value()->mutable_fields());

  Protobuf::Value topic_v;
  topic_v.set_string_value(topic_.c_str(), topic_.length());
  members["topic"] = topic_v;
}

bool GetRouteInfoRequestHeader::decode(const Protobuf::Value& ext_fields) {
  return readString(ext_fields.struct_value().fields(), "topic", topic_);
}

void PopMessageRequestHeader::encode(Protobuf::Value& root) {
  auto& members = *(root.mutable_struct_value()->mutable_fields());

  ASSERT(!consumer_group_.empty());
  Protobuf::Value consumer_group_v;
  consumer_group_v.set_string_value(consumer_group_.c_str(), consumer_group_.size());
  members["consumerGroup"] = consumer_group_v;

  ASSERT(!topic_.empty());
  Protobuf::Value topicNode;
  topicNode.set_string_value(topic_.c_str(), topic_.length());
  members["topic"] = topicNode;

  Protobuf::Value queue_id_v;
  queue_id_v.set_number_value(queue_id_);
  members["queueId"] = queue_id_v;

  Protobuf::Value max_msg_nums_v;
  max_msg_nums_v.set_number_value(max_msg_nums_);
  members["maxMsgNums"] = max_msg_nums_v;

  Protobuf::Value invisible_time_v;
  invisible_time_v.set_number_value(invisible_time_);
  members["invisibleTime"] = invisible_time_v;

  Protobuf::Value poll_time_v;
  poll_time_v.set_number_value(poll_time_);
  members["pollTime"] = poll_time_v;

  Protobuf::Value born_time_v;
  born_time_v.set_number_value(born_time_);
  members["bornTime"] = born_time_v;

  Protobuf::Value init_mode_v;
  init_mode_v.set_number_value(init_mode_);
  members["initMode"] = init_mode_v;

  if (!exp_type_.empty()) {
    Protobuf::Value exp_type_v;
    exp_type_v.set_string_value(exp_type_.c_str(), exp_type_.size());
    members["expType"] = exp_type_v;
  }

  if (!exp_.empty()) {
    Protobuf::Value exp_v;
    exp_v.set_string_value(exp_.c_str(), exp_.size());
    members["exp"] = exp_v;
  }
}

bool PopMessageRequestHeader::decode(const Protobuf::Value& ext_fields) {
  const auto& members = ext_fields.struct_value().fields();
  if (!readString(members, "consumerGroup", consumer_group_) ||
      !readString(members, "topic", topic_) || !readInt(members, "queueId", queue_id_) ||
      !readInt(members, "maxMsgNums", max_msg_nums_) ||
      !readInt(members, "invisibleTime", invisible_time_) ||
      !readInt(members, "pollTime", poll_time_) || !readInt(members, "bornTime", born_time_) ||
      !readInt(members, "initMode", init_mode_)) {
    return false;
  }

  if (members.contains("expType")) {
    exp_type_ = members.at("expType").string_value();
  }

  if (members.contains("exp")) {
    exp_ = members.at("exp").string_value();
  }
  return true;
}

void PopMessageResponseHeader::encode(Protobuf::Value& root) {
  auto& members = *(root.mutable_struct_value()->mutable_fields());

  Protobuf::Value pop_time_v;
  pop_time_v.set_number_value(pop_time_);
  members["popTime"] = pop_time_v;

  Protobuf::Value invisible_time_v;
  invisible_time_v.set_number_value(invisible_time_);
  members["invisibleTime"] = invisible_time_v;

  Protobuf::Value revive_qid_v;
  revive_qid_v.set_number_value(revive_qid_);
  members["reviveQid"] = revive_qid_v;

  Protobuf::Value rest_num_v;
  rest_num_v.set_number_value(rest_num_);
  members["restNum"] = rest_num_v;

  if (!start_offset_info_.empty()) {
    Protobuf::Value start_offset_info_v;
    start_offset_info_v.set_string_value(start_offset_info_.c_str(), start_offset_info_.size());
    members["startOffsetInfo"] = start_offset_info_v;
  }

  if (!msg_off_set_info_.empty()) {
    Protobuf::Value msg_offset_info_v;
    msg_offset_info_v.set_string_value(msg_off_set_info_.c_str(), msg_off_set_info_.size());
    members["msgOffsetInfo"] = msg_offset_info_v;
  }

  if (!order_count_info_.empty()) {
    Protobuf::Value order_count_info_v;
    order_count_info_v.set_string_value(order_count_info_.c_str(), order_count_info_.size());
    members["orderCountInfo"] = order_count_info_v;
  }
}

bool PopMessageResponseHeader::decode(const Protobuf::Value& ext_fields) {
  const auto& members = ext_fields.struct_value().fields();
  if (!readInt(members, "popTime", pop_time_) ||
      !readInt(members, "invisibleTime", invisible_time_) ||
      !readInt(members, "reviveQid", revive_qid_) || !readInt(members, "restNum", rest_num_)) {
    return false;
  }

  if (members.contains("startOffsetInfo")) {
    start_offset_info_ = members.at("startOffsetInfo").string_value();
  }

  if (members.contains("msgOffsetInfo")) {
    msg_off_set_info_ = members.at("msgOffsetInfo").string_value();
  }

  if (members.contains("orderCountInfo")) {
    order_count_info_ = members.at("orderCountInfo").string_value();
  }
  return true;
}

void AckMessageRequestHeader::encode(Protobuf::Value& root) {
  auto& members = *(root.mutable_struct_value()->mutable_fields());

  ASSERT(!consumer_group_.empty());
  Protobuf::Value consumer_group_v;
  consumer_group_v.set_string_value(consumer_group_.c_str(), consumer_group_.size());
  members["consumerGroup"] = consumer_group_v;

  ASSERT(!topic_.empty());
  Protobuf::Value topic_v;
  topic_v.set_string_value(topic_.c_str(), topic_.size());
  members["topic"] = topic_v;

  ASSERT(queue_id_ >= 0);
  Protobuf::Value queue_id_v;
  queue_id_v.set_number_value(queue_id_);
  members["queueId"] = queue_id_v;

  ASSERT(!extra_info_.empty());
  Protobuf::Value extra_info_v;
  extra_info_v.set_string_value(extra_info_.c_str(), extra_info_.size());
  members["extraInfo"] = extra_info_v;

  ASSERT(offset_ >= 0);
  Protobuf::Value offset_v;
  offset_v.set_number_value(offset_);
  members["offset"] = offset_v;
}

bool AckMessageRequestHeader::decode(const Protobuf::Value& ext_fields) {
  const auto& members = ext_fields.struct_value().fields();
  return readString(members, "consumerGroup", consumer_group_) &&
         readString(members, "topic", topic_) && readInt(members, "queueId", queue_id_) &&
         readString(members, "extraInfo", extra_info_) && readInt(members, "offset", offset_);
}

void UnregisterClientRequestHeader::encode(Protobuf::Value& root) {
  auto& members = *(root.mutable_struct_value()->mutable_fields());

  ASSERT(!client_id_.empty());
  Protobuf::Value client_id_v;
  client_id_v.set_string_value(client_id_.c_str(), client_id_.size());
  members["clientID"] = client_id_v;

  ASSERT(!producer_group_.empty() || !consumer_group_.empty());
  if (!producer_group_.empty()) {
    Protobuf::Value producer_group_v;
    producer_group_v.set_string_value(producer_group_.c_str(), producer_group_.size());
    members["producerGroup"] = producer_group_v;
  }

  if (!consumer_group_.empty()) {
    Protobuf::Value consumer_group_v;
    consumer_group_v.set_string_value(consumer_group_.c_str(), consumer_group_.size());
    members["consumerGroup"] = consumer_group_v;
  }
}

bool UnregisterClientRequestHeader::decode(const Protobuf::Value& ext_fields) {
  const auto& members = ext_fields.struct_value().fields();
  if (!readString(members, "clientID", client_id_) ||
      (!members.contains("producerGroup") && !members.contains("consumerGroup"))) {
    return false;
  }

  if (members.contains("consumerGroup")) {
    consumer_group_ = members.at("consumerGroup").string_value();
  }

  if (members.contains("producerGroup")) {
    producer_group_ = members.at("producerGroup").string_value();
  }
  return true;
}

void GetConsumerListByGroupResponseBody::encode(Protobuf::Struct& root) {
  auto& members = *(root.mutable_fields());

  Protobuf::Value consumer_id_list_v;
  auto member_list = consumer_id_list_v.mutable_list_value();
  for (const auto& consumerId : consumer_id_list_) {
    auto consumer_id_v = new Protobuf::Value;
    consumer_id_v->set_string_value(consumerId.c_str(), consumerId.size());
    member_list->mutable_values()->AddAllocated(consumer_id_v);
  }
  members["consumerIdList"] = consumer_id_list_v;
}

bool HeartbeatData::decode(Protobuf::Struct& doc) {
  const auto& members = doc.fields();
  if (!members.contains("clientID")) {
    return false;
  }

  client_id_ = members.at("clientID").string_value();

  if (members.contains("consumerDataSet")) {
    auto& consumer_data_list = members.at("consumerDataSet").list_value().values();
    for (const auto& it : consumer_data_list) {
      if (it.struct_value().fields().contains("groupName")) {
        consumer_groups_.push_back(it.struct_value().fields().at("groupName").string_value());
      }
    }
  }
  return true;
}

void HeartbeatData::encode(Protobuf::Struct& root) {
  auto& members = *(root.mutable_fields());

  Protobuf::Value client_id_v;
  client_id_v.set_string_value(client_id_.c_str(), client_id_.size());
  members["clientID"] = client_id_v;
}

void GetConsumerListByGroupRequestHeader::encode(Protobuf::Value& root) {
  auto& members = *(root.mutable_struct_value()->mutable_fields());

  Protobuf::Value consumer_group_v;
  consumer_group_v.set_string_value(consumer_group_.c_str(), consumer_group_.size());
  members["consumerGroup"] = consumer_group_v;
}

bool GetConsumerListByGroupRequestHeader::decode(const Protobuf::Value& ext_fields) {
  return readString(ext_fields.struct_value().fields(), "consumerGroup", consumer_group_);
}

void MetadataHelper::parseRequest(RemotingCommandPtr& request, MessageMetadataSharedPtr metadata) {
  metadata->setOneWay(request->isOneWay());
  CommandCustomHeader* custom_header = request->customHeader();

  auto route_command_custom_header = request->typedCustomHeader<RoutingCommandCustomHeader>();
  if (route_command_custom_header != nullptr) {
    metadata->setTopicName(route_command_custom_header->topic());
  }

  const uint64_t code = request->code();
  metadata->headers().addCopy(Http::LowerCaseString("code"), code);

  if (enumToInt(RequestCode::AckMessage) == code) {
    metadata->headers().addCopy(Http::LowerCaseString(RocketmqConstants::get().BrokerName),
                                custom_header->targetBrokerName());
    metadata->headers().addCopy(Http::LowerCaseString(RocketmqConstants::get().BrokerId),
                                custom_header->targetBrokerId());
  }
}

} // namespace RocketmqProxy
} // namespace NetworkFilters
} // namespace Extensions
} // namespace Envoy
