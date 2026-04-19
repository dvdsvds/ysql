#include "buffer_pool.h"
#include <stdexcept>

using namespace ysql;

uint8_t* BufferPool::get_page(uint32_t page_id) {
    if(page_cache.find(page_id) != page_cache.end()) {
        page_cache[page_id].pin_count++;
        return page_cache[page_id].bytes.data();
    } else {
        if(page_cache.size() >= max_size) {
            uint32_t evict_id = 0;
            bool found = false;
            for(auto& pair : page_cache) {
                if(pair.second.pin_count == 0) {
                    evict_id = pair.first;
                    found = true;
                    break;
                }
            }
            if(!found) {
                throw std::runtime_error("all pages pinned");
            }
            if(dirty.count(evict_id)) {
                pager->write(evict_id, page_cache[evict_id].bytes.data());
                dirty.erase(evict_id);
            }
            page_cache.erase(evict_id);
        }
        pager->read(page_id, page_cache[page_id].bytes.data());
        page_cache[page_id].pin_count++;
        return page_cache[page_id].bytes.data();
    }
}

void BufferPool::unpin_page(uint32_t page_id) {
    if(page_cache.find(page_id) == page_cache.end()) {
        throw std::runtime_error("unpin: page not in cache");
    }

    if(page_cache[page_id].pin_count == 0) {
        throw std::runtime_error("unpin: pin_count already zero");
    }

    page_cache[page_id].pin_count--;
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
        pager->write(d, page_cache[d].bytes.data());
    }
    dirty.clear();
}
