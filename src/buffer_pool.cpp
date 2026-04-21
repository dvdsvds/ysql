#include "buffer_pool.h"
#include <iostream>
#include <memory>
#include <stdexcept>

using namespace ysql;

uint8_t* BufferPool::get_page(uint32_t page_id) {
    static uint64_t hit_count = 0;
    static uint64_t miss_count = 0;
    if(page_cache.find(page_id) != page_cache.end()) {
        hit_count++;
        page_cache[page_id]->pin_count++;
        return page_cache[page_id]->bytes.data();
    } else {
        if(page_cache.size() >= max_size) {
            uint32_t evict_id = 0;
            bool found = false;
            for(auto& pair : page_cache) {
                if(pair.second->pin_count == 0) {
                    evict_id = pair.first;
                    found = true;
                    break;
                }
            }
            if(!found) {
                // std::cerr << "\n=== all pages pinned! Dump v2 ===" << std::endl;
                // std::cerr << "requested page_id = " << page_id << std::endl;
                // std::cerr << "page_cache.size() = " << page_cache.size() << std::endl;
                // for(auto& pair : page_cache) {
                //     std::cerr << "  page " << pair.first << " pin_count=" << pair.second->pin_count << std::endl;
                // }
                std::cerr << "hit_count = " << hit_count << std::endl;
                std::cerr << "miss_count = " << miss_count << std::endl;
                throw std::runtime_error("all pages pinned");
            }
            if(dirty.count(evict_id)) {
                pager->write(evict_id, page_cache[evict_id]->bytes.data());
                dirty.erase(evict_id);
            }
            page_cache.erase(evict_id);
        }
        miss_count++;
        page_cache[page_id] = std::make_unique<Buffer>();
        pager->read(page_id, page_cache[page_id]->bytes.data());
        page_cache[page_id]->pin_count++;
        return page_cache[page_id]->bytes.data();
    }
}

void BufferPool::unpin_page(uint32_t page_id) {
    if(page_cache.find(page_id) == page_cache.end()) {
        throw std::runtime_error("unpin: page not in cache");
    }

    if(page_cache[page_id]->pin_count == 0) {
        throw std::runtime_error("unpin: pin_count already zero");
    }

    page_cache[page_id]->pin_count--;
}

bool BufferPool::make_dirty(uint32_t page_id) {
    if(page_cache.find(page_id) != page_cache.end()) {
        dirty.insert(page_id);
        return true;
    }
    return false;
}

void BufferPool::flush() {
    for(const auto& d : dirty) {
        pager->write(d, page_cache[d]->bytes.data());
    }
    dirty.clear();
}
