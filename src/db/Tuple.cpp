#include <db/Tuple.hpp>

#include <cstring>
#include <stdexcept>
#include <unordered_set>
#include <iostream>

using namespace db;

// ---------------- 辅助函数 ----------------
static std::string type_to_string(type_t t) {
    switch (t) {
        case type_t::INT: return "INT";
        case type_t::DOUBLE: return "DOUBLE";
        case type_t::CHAR: return "CHAR";
        default: return "UNKNOWN";
    }
}

// ---------------- Tuple ----------------
Tuple::Tuple(const std::vector<field_t>& fields) : fields_(fields) {}

type_t Tuple::field_type(size_t i) const {
  const field_t& f = fields_.at(i);
  if (std::holds_alternative<int>(f))            return type_t::INT;
  if (std::holds_alternative<double>(f))         return type_t::DOUBLE;
  if (std::holds_alternative<std::string>(f))    return type_t::CHAR;
  throw std::logic_error("Tuple: unknown field type");
}

size_t Tuple::size() const { return fields_.size(); }

const field_t& Tuple::get_field(size_t i) const { return fields_.at(i); }

// ---------------- TupleDesc ----------------
TupleDesc::TupleDesc(const std::vector<type_t>& types,
                     const std::vector<std::string>& names) {
  if (types.size() != names.size()) {
    throw std::logic_error("TupleDesc: types and names must have same length");
  }

  std::unordered_set<std::string> seen;
  for (const auto& n : names) {
    if (!seen.insert(n).second) {
      throw std::logic_error("TupleDesc: duplicate field name: " + n);
    }
  }

  types_ = types;
  names_ = names;
  offsets_.resize(types_.size());

  auto size_of_fixed = [](type_t t) -> size_t {
    switch (t) {
      case type_t::INT:    return INT_SIZE;
      case type_t::DOUBLE: return DOUBLE_SIZE;
      case type_t::CHAR:   return CHAR_SIZE;
    }
    throw std::logic_error("TupleDesc: unknown type");
  };

  size_t off = 0;
  for (size_t i = 0; i < types_.size(); ++i) {
    offsets_[i] = off;
    off += size_of_fixed(types_[i]);
    name2idx_.emplace(names_[i], i);
  }
  length_ = off;
}

bool TupleDesc::compatible(const Tuple& tuple) const {
  if (tuple.size() != types_.size()) {
      std::cerr << "[DEBUG] Compatible Mismatch: Size mismatch! Schema: "
                << types_.size() << ", Tuple: " << tuple.size() << std::endl;
      return false;
  }
  for (size_t i = 0; i < types_.size(); ++i) {
    if (tuple.field_type(i) != types_[i]) {
        std::cerr << "[DEBUG] Compatible Mismatch at index " << i << ": "
                  << "Expected " << type_to_string(types_[i]) << ", "
                  << "Got " << type_to_string(tuple.field_type(i)) << std::endl;
        return false;
    }
  }
  return true;
}

size_t TupleDesc::index_of(const std::string& name) const {
  auto it = name2idx_.find(name);
  if (it == name2idx_.end()) {
    throw std::out_of_range("TupleDesc::index_of: field not found: " + name);
  }
  return it->second;
}

size_t TupleDesc::offset_of(const size_t& index) const {
  if (index >= offsets_.size()) {
    throw std::out_of_range("TupleDesc::offset_of: index out of range");
  }
  return offsets_[index];
}

size_t TupleDesc::length() const { return length_; }
size_t TupleDesc::size()   const { return types_.size(); }

type_t TupleDesc::field_type(size_t i) const {
    if (i >= types_.size()) {
        throw std::out_of_range("TupleDesc::field_type: index out of range");
    }
    return types_[i];
}

Tuple TupleDesc::deserialize(const uint8_t* data) const {
  std::vector<field_t> out;
  out.reserve(types_.size());

  for (size_t i = 0; i < types_.size(); ++i) {
    const uint8_t* ptr = data + offsets_[i];
    switch (types_[i]) {
      case type_t::INT: {
        int v;
        std::memcpy(&v, ptr, INT_SIZE);
        out.emplace_back(v);
        break;
      }
      case type_t::DOUBLE: {
        double v;
        std::memcpy(&v, ptr, DOUBLE_SIZE);
        out.emplace_back(v);
        break;
      }
      case type_t::CHAR: {
        const char* csrc = reinterpret_cast<const char*>(ptr);
        size_t len = 0;
        while (len < CHAR_SIZE && csrc[len] != '\0') ++len;
        out.emplace_back(std::string(csrc, len));
        break;
      }
    }
  }
  return Tuple(out);
}

void TupleDesc::serialize(uint8_t* data, const Tuple& t) const {
  if (!compatible(t)) {
    throw std::logic_error("TupleDesc::serialize: tuple incompatible with schema");
  }

  for (size_t i = 0; i < types_.size(); ++i) {
    uint8_t* ptr = data + offsets_[i];
    switch (types_[i]) {
      case type_t::INT: {
        const int& v = std::get<int>(t.get_field(i));
        std::memcpy(ptr, &v, INT_SIZE);
        break;
      }
      case type_t::DOUBLE: {
        const double& v = std::get<double>(t.get_field(i));
        std::memcpy(ptr, &v, DOUBLE_SIZE);
        break;
      }
      case type_t::CHAR: {
        const std::string& s = std::get<std::string>(t.get_field(i));
        size_t n = s.size();
        if (n > CHAR_SIZE) n = CHAR_SIZE;
        std::memcpy(ptr, s.data(), n);
        if (n < CHAR_SIZE) std::memset(ptr + n, 0, CHAR_SIZE - n);
        break;
      }
    }
  }
}

TupleDesc TupleDesc::merge(const TupleDesc& td1, const TupleDesc& td2) {
  std::vector<type_t> types;
  std::vector<std::string> names;
  types.reserve(td1.size() + td2.size());
  names.reserve(td1.size() + td2.size());

  for (size_t i = 0; i < td1.size(); ++i) {
    types.push_back(td1.types_[i]);
    names.push_back(td1.names_[i]);
  }
  for (size_t i = 0; i < td2.size(); ++i) {
    types.push_back(td2.types_[i]);
    names.push_back(td2.names_[i]);
  }
  return TupleDesc(types, names);
}