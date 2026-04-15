#pragma once

#include <cstdint>
#include <string>
#include "page.h"
namespace ysql {
    struct FileHeader {
        uint32_t magic_num = 0x5953514C;
        uint16_t version = 1;
        uint32_t page_count = 1;
        uint32_t free_list_header = 0;
    };

    class Pager {
        private:
            FileHeader file_header;
            int fd = -1;
        public:
            int open(const std::string& path);
            int read(uint32_t page_id, uint8_t* buf);
            int write(uint32_t page_id, const uint8_t* buf);
            uint32_t allocate();
            int close();
    };
}
