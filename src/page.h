#pragma once

#include <cstdint>
namespace ysql {
    constexpr uint32_t PAGE_SIZE = 4 * 1024;
    constexpr uint16_t PAGE_HEADER_SIZE = 6;

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

    struct Cell {
        uint32_t key;
        uint32_t value;
    };

    constexpr uint16_t MAX_CELLS = (PAGE_SIZE - PAGE_HEADER_SIZE) / sizeof(Cell);

    struct InternalHeader {
        PageHeader base;
        uint32_t left_child;
    };

    struct InternalCell {
        uint32_t key;
        uint32_t child_page_id;
    };
}
