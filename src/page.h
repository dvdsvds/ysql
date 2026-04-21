#pragma once

#include <cstdint>
namespace ysql {
    constexpr uint32_t PAGE_SIZE = 4 * 1024;

    enum class PAGE_TYPE : uint8_t {
        FILE_HEADER = 0,
        LEAF = 1,
        INTERNAL = 2,
        FREE = 3
    };

    struct PageHeader {
        PAGE_TYPE page_type;
        uint16_t cell_count;
        uint16_t free_space_offset;
    };

    struct InternalHeader {
        PageHeader base;
        uint32_t left_child_id;
    };

    struct InternalCell {
        uint32_t key;
        uint32_t child_page_id;
    };

    struct LeafHeader {
        PageHeader base;
        uint32_t next_leaf;
    };

    struct LeafCell {
        uint32_t key;
        uint32_t value;
    };

    constexpr uint16_t MAX_LEAF_CELLS = (PAGE_SIZE - sizeof(LeafHeader)) / sizeof(LeafCell);
    constexpr uint16_t MAX_INTERNAL_CELLS = (PAGE_SIZE - sizeof(InternalHeader)) / sizeof(InternalCell);
}
