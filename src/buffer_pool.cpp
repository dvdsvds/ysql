#include "buffer_pool.h"

using namespace ysql;

uint8_t* BufferPool::get_page(uint32_t page_id) {
    if(page_cache.find(page_id) != page_cache.end()) {
        return page_cache[page_id].data();
    } else {
        if(page_cache.size() >= max_size) {
            uint32_t evict_id = page_cache.begin()->first;
            if(dirty.count(evict_id)) {
                pager->write(evict_id, page_cache[evict_id].data());
                dirty.erase(evict_id);
            }
            page_cache.erase(evict_id);
        }
        pager->read(page_id, page_cache[page_id].data());
        return page_cache[page_id].data();
    }
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
        pager->write(d, page_cache[d].data());
    }
    dirty.clear();
}
