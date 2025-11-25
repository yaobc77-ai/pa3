#include <db/Query.hpp>
#include <db/Tuple.hpp>
#include <unordered_map>
#include <map>
#include <limits>
#include <algorithm>
#include <stdexcept>
#include <cmath>
#include <iostream>

using namespace db;

namespace {

// 辅助比较函数
bool compare_fields(const field_t &lhs, const field_t &rhs, PredicateOp op) {
    if (lhs.index() != rhs.index()) {
        if (std::holds_alternative<int>(lhs) && std::holds_alternative<double>(rhs)) {
            return compare_fields(static_cast<double>(std::get<int>(lhs)), rhs, op);
        }
        if (std::holds_alternative<double>(lhs) && std::holds_alternative<int>(rhs)) {
            return compare_fields(lhs, static_cast<double>(std::get<int>(rhs)), op);
        }
        return false;
    }

    auto cmp = [op](const auto &a, const auto &b) {
        switch (op) {
            case PredicateOp::EQ: return a == b;
            case PredicateOp::NE: return a != b;
            case PredicateOp::LT: return a < b;
            case PredicateOp::LE: return a <= b;
            case PredicateOp::GT: return a > b;
            case PredicateOp::GE: return a >= b;
        }
        return false;
    };

    if (std::holds_alternative<int>(lhs)) {
        return cmp(std::get<int>(lhs), std::get<int>(rhs));
    } else if (std::holds_alternative<double>(lhs)) {
        return cmp(std::get<double>(lhs), std::get<double>(rhs));
    } else {
        return cmp(std::get<std::string>(lhs), std::get<std::string>(rhs));
    }
}

struct Aggregator {
    AggregateOp op;
    int count = 0;
    double sum = 0.0;
    field_t min_val;
    field_t max_val;
    bool is_initialized = false;
    bool source_is_int = false;

    void add(const field_t &val) {
        count++;
        if (!is_initialized) {
            source_is_int = std::holds_alternative<int>(val);
            min_val = val;
            max_val = val;
            is_initialized = true;
        }

        double num_val = 0.0;
        if (std::holds_alternative<int>(val)) {
            num_val = static_cast<double>(std::get<int>(val));
        } else if (std::holds_alternative<double>(val)) {
            num_val = std::get<double>(val);
        }
        sum += num_val;

        if (op == AggregateOp::MIN) {
            if (compare_fields(val, min_val, PredicateOp::LT)) min_val = val;
        }
        if (op == AggregateOp::MAX) {
            if (compare_fields(val, max_val, PredicateOp::GT)) max_val = val;
        }
    }

    field_t get_result() const {
        switch (op) {
            case AggregateOp::COUNT: return count;
            case AggregateOp::AVG: return (count == 0) ? 0.0 : (sum / count);
            case AggregateOp::SUM:
                return source_is_int ? static_cast<int>(sum) : sum;
            case AggregateOp::MIN: return min_val;
            case AggregateOp::MAX: return max_val;
        }
        return 0;
    }
};

} // namespace

// ================== 实现部分 ==================

void db::projection(const DbFile &in, DbFile &out, const std::vector<std::string> &field_names) {
    const TupleDesc &in_td = in.getTupleDesc();
    std::vector<size_t> indices;
    indices.reserve(field_names.size());
    for (const auto &name : field_names) {
        indices.push_back(in_td.index_of(name));
    }

    for (auto it = in.begin(); it != in.end(); ++it) {
        Tuple t = *it;
        std::vector<field_t> new_fields;
        new_fields.reserve(indices.size());
        for (size_t idx : indices) {
            new_fields.push_back(t.get_field(idx));
        }
        out.insertTuple(Tuple(new_fields));
    }
}

void db::filter(const DbFile &in, DbFile &out, const std::vector<FilterPredicate> &pred) {
    const TupleDesc &in_td = in.getTupleDesc();
    struct CachedPred {
        size_t field_idx;
        PredicateOp op;
        field_t value;
    };
    std::vector<CachedPred> cached_preds;
    for (const auto &p : pred) {
        cached_preds.push_back({in_td.index_of(p.field_name), p.op, p.value});
    }

    for (auto it = in.begin(); it != in.end(); ++it) {
        Tuple t = *it;
        bool satisfied = true;
        for (const auto &p : cached_preds) {
            field_t tuple_val = t.get_field(p.field_idx);
            if (!compare_fields(tuple_val, p.value, p.op)) {
                satisfied = false;
                break;
            }
        }
        if (satisfied) out.insertTuple(t);
    }
}

void db::aggregate(const DbFile &in, DbFile &out, const Aggregate &agg) {
    const TupleDesc &in_td = in.getTupleDesc();
    size_t agg_field_idx = in_td.index_of(agg.field);

    // 1. 获取源类型 (INT or DOUBLE)
    // 之前报错的地方：现在应该能通过编译了，因为你更新了 Tuple.hpp/cpp
    type_t src_type = in_td.field_type(agg_field_idx);
    bool src_is_int = (src_type == type_t::INT);

    std::map<field_t, Aggregator> groups;
    bool has_group = agg.group.has_value();
    size_t group_field_idx = 0;
    if (has_group) {
        group_field_idx = in_td.index_of(agg.group.value());
    }

    // 扫描数据
    for (auto it = in.begin(); it != in.end(); ++it) {
        Tuple t = *it;
        field_t key = has_group ? t.get_field(group_field_idx) : 0;

        Aggregator &ag = groups[key];
        if (ag.count == 0) ag.op = agg.op;
        ag.add(t.get_field(agg_field_idx));
    }

    // 2. 空数据处理逻辑 (你的报错就是在这里触发的)
    if (groups.empty() && !has_group) {
        if (agg.op == AggregateOp::COUNT) {
            out.insertTuple(Tuple({0}));
        } else if (agg.op == AggregateOp::AVG) {
            out.insertTuple(Tuple({0.0}));
        } else {
            // 修复：根据 Schema 决定返回 INT 还是 DOUBLE
            if (src_is_int) out.insertTuple(Tuple({0}));   // 对应 INTSchema
            else out.insertTuple(Tuple({0.0}));            // 对应 DOUBLE Schema
        }
        return;
    }

    // 3. 正常数据处理
    for (const auto &[key, ag] : groups) {
        std::vector<field_t> result_fields;
        if (has_group) {
            result_fields.push_back(key);
        }
        result_fields.push_back(ag.get_result());
        out.insertTuple(Tuple(result_fields));
    }
}

void db::join(const DbFile &left, const DbFile &right, DbFile &out, const JoinPredicate &pred) {
    const TupleDesc &left_td = left.getTupleDesc();
    const TupleDesc &right_td = right.getTupleDesc();
    size_t left_idx = left_td.index_of(pred.left);
    size_t right_idx = right_td.index_of(pred.right);

    for (auto left_it = left.begin(); left_it != left.end(); ++left_it) {
        Tuple left_tuple = *left_it;
        field_t left_val = left_tuple.get_field(left_idx);

        for (auto right_it = right.begin(); right_it != right.end(); ++right_it) {
            Tuple right_tuple = *right_it;
            field_t right_val = right_tuple.get_field(right_idx);

            if (compare_fields(left_val, right_val, pred.op)) {
                std::vector<field_t> new_fields;
                for (size_t i = 0; i < left_tuple.size(); ++i) new_fields.push_back(left_tuple.get_field(i));
                for (size_t i = 0; i < right_tuple.size(); ++i) {
                    if (pred.op == PredicateOp::EQ && i == right_idx) continue;
                    new_fields.push_back(right_tuple.get_field(i));
                }
                out.insertTuple(Tuple(new_fields));
            }
        }
    }
}