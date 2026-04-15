#pragma once

#include "src/page.h"
#include "src/pager.h"
#include <optional>
namespace ysql {
    class BTree {
        private:
            Pager* pager;
            uint32_t root_page_id;
        public:
            BTree(Pager* pager, uint32_t root_page_id) : pager(pager), root_page_id(root_page_id) {}
            void insert(const Cell& cell);
            std::optional<uint32_t> search(const uint32_t& key);
            void split(uint32_t page_id);
            uint32_t find_leaf(uint32_t key);
            void remove(const uint32_t& key);
    };
}
