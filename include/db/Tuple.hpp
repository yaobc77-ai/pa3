#pragma once

#include <db/types.hpp>
#include <string>
#include <vector>
#include <unordered_map>
#include <variant>
#include <cstdint>

namespace db {

    class Tuple {
        std::vector<field_t> fields_;

    public:
        Tuple(const std::vector<field_t>& fields);

        type_t         field_type(size_t i) const;
        size_t         size() const;
        const field_t& get_field(size_t i) const;
    };

    // ---------------- TupleDesc ----------------
    class TupleDesc {
    private:
        std::vector<type_t>      types_;
        std::vector<std::string> names_;
        std::vector<size_t>      offsets_;
        size_t                   length_{0};
        std::unordered_map<std::string, size_t> name2idx_;

    public:
        TupleDesc() = default;

        TupleDesc(const std::vector<type_t>& types,
                  const std::vector<std::string>& names);

        bool   compatible(const Tuple& tuple) const;

        size_t offset_of(const size_t& index) const;

        size_t index_of(const std::string& name) const;

        size_t size() const;

        size_t length() const;

        type_t field_type(size_t i) const;

        void   serialize(uint8_t* data, const Tuple& t) const;
        Tuple  deserialize(const uint8_t* data) const;

        static TupleDesc merge(const TupleDesc& td1, const TupleDesc& td2);
    };

} // namespace db