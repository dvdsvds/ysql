#pragma once

#include "src/page.h"
#include "src/pager.h"
#include <array>
#include <cstdint>
#include <memory>
#include <unordered_map>
#include <unordered_set>
namespace ysql {
    class BufferPool {
        private:
            struct Buffer {
                std::array<uint8_t, PAGE_SIZE> bytes;
                uint32_t pin_count = 0;
            };
            std::unordered_map<uint32_t, std::unique_ptr<Buffer>> page_cache;
            std::unordered_set<uint32_t> dirty;
            uint32_t max_size;
            Pager* pager;
        public:
            BufferPool(Pager* pager, uint32_t max_size) : pager(pager), max_size(max_size) {}
            uint8_t* get_page(uint32_t page_id);
            void unpin_page(uint32_t page_id);
            bool make_dirty(uint32_t page_id);
            void flush();
    };
}
