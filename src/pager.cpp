#include "pager.h"
#include "page.h"
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <unistd.h>

using namespace ysql;

int Pager::open(const std::string& path) {
    fd = ::open(path.c_str(), O_RDWR | O_CREAT, 0644);
    if(fd == -1) {
        std::cerr << "Open failed" << std::endl;
        return -1;
    }

    if(lseek(fd, 0, SEEK_END) == 0) {
        uint8_t buf[PAGE_SIZE] = {};
        std::memcpy(buf, &file_header, sizeof(FileHeader));
        pwrite(fd, buf, PAGE_SIZE, 0);
    } else {
        uint8_t buf[PAGE_SIZE];
        pread(fd, buf, PAGE_SIZE, 0);
        std::memcpy(&file_header, buf, sizeof(FileHeader));
        
        if(file_header.magic_num != 0x5953514C) {
            return -1;
        }
    }

    return 0;
}


int Pager::read(uint32_t page_id, uint8_t* buf) {
    if(pread(this->fd, buf, PAGE_SIZE, page_id * PAGE_SIZE) != PAGE_SIZE) {
        return -1;
    }
    return 0;
}

int Pager::write(uint32_t page_id, const uint8_t* buf) {
    if(pwrite(this->fd, buf, PAGE_SIZE, page_id * PAGE_SIZE) != PAGE_SIZE) {
        return -1;
    }
    return 0;
}

uint32_t Pager::allocate() {
    uint32_t page_id = file_header.page_count++;
    uint8_t buf[PAGE_SIZE] = {};
    std::memcpy(buf, &file_header, sizeof(FileHeader));
    pwrite(this->fd, buf, PAGE_SIZE, 0);
    return page_id;
}

int Pager::close() {
    if(::close(this->fd) == -1) {
        return -1;
    }
    this->fd = -1;
    return 0;
}
