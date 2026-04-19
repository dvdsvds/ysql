#pragma once

#include "src/buffer_pool.h"
#include "src/page.h"
#include "src/pager.h"
#include <optional>
#include <vector>
namespace ysql {
    struct PathEntry {
        uint32_t page_id;
        uint32_t child_index;
    };

    class BTree {
        private:
            Pager* pager;
            BufferPool* buffer_pool;
            uint32_t root_page_id;
        public:
            BTree(BufferPool* buffer_pool, Pager* pager, uint32_t root_page_id) : buffer_pool(buffer_pool), pager(pager), root_page_id(root_page_id) {}
            void insert(const Cell& cell);
            std::optional<uint32_t> search(const uint32_t& key);
            void split(uint32_t page_id, const std::vector<PathEntry>& path);
            void split_internal(uint32_t page_id, const std::vector<PathEntry>& path);
            void insert_into_parent(uint32_t parent_id, uint32_t child_index, uint32_t split_key, uint32_t new_page_id, const std::vector<PathEntry>& path);
            uint32_t find_leaf(uint32_t key);
            uint32_t find_leaf(uint32_t key, std::vector<PathEntry>& path);
            void remove(const uint32_t& key);
    };
}
