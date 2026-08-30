#pragma once

#include <cstdint>
#include <string>

namespace pipeline {

// A single unit of work flowing through the pipeline. Deliberately simple
// (a log-processing style record) -- the point of this project is the
// queue/backpressure mechanics, not the payload.
struct Record {
    uint64_t id = 0;
    std::string raw;      // e.g. raw log line, filled in by Reader
    std::string field;    // e.g. parsed field, filled in by Parser
    bool keep = true;      // e.g. filter decision, filled in by Filter

    Record() = default;
    explicit Record(uint64_t id_, std::string raw_ = "")
        : id(id_), raw(std::move(raw_)) {}
};

} // namespace pipeline
