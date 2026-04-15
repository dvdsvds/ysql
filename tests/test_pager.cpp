#include "src/pager.h"
#include "src/btree.h"
#include <cstring>
#include <iostream>
#include <cstdio>

using namespace ysql;
using namespace std;

void test1() {
    Pager pager;
    if(pager.open("test.db") == -1) {
        cerr << "Open failed" << endl;
    }
    pager.close();
    if(pager.open("test.db") == -1) {
        cerr << "Open failed" << endl;
    }

    cout << "TEST PASS" << endl; 

}

void test2() {
    Pager pager;
    pager.open("test.db");
    uint32_t page_id = pager.allocate();
    cout << "page id: " << page_id << endl;

    uint8_t w_buf[PAGE_SIZE] = {};
    std::memcpy(w_buf, "hello ysql", 10);
    pager.write(page_id, w_buf);
    cout << "w: " << w_buf << endl;

    uint8_t r_buf[PAGE_SIZE] = {};
    pager.read(page_id, r_buf);
    cout << "r: " << r_buf << endl;

    if(std::memcmp(w_buf, r_buf, PAGE_SIZE) == 0) {
        cout << "TEST PASS" << endl;
    }
}

void test3() {
    std::remove("test.db");
    Pager pager;
    int fd1 = pager.open("test.db");
    uint32_t page_id1 = pager.allocate();
    cout << "page_id: " << page_id1 << endl;
    pager.close();

    int fd2 = pager.open("test.db");
    uint32_t page_id2 = pager.allocate();
    cout << "page_id: " << page_id2 << endl;
    
    if(page_id2 > page_id1) {
        cout << "TEST PASS" << endl;
    } else {
        cout << "TEST Failed" << endl;
    }
}

void test4() {
    Pager pager;
    int fd = pager.open("test.db");
    uint32_t page_id = pager.allocate();
    uint8_t buf[PAGE_SIZE] = {};
    PageHeader* header = reinterpret_cast<PageHeader*>(buf);
    header->page_type = PAGE_TYPE::LEAF;
    header->cell_count = 0;
    header->free_space_offset = PAGE_HEADER_SIZE;
    pager.write(page_id, buf);
    BTree bt(&pager, page_id);
    bt.insert({3, 300});
    bt.insert({9, 100000});
    bt.insert({5, 500});
    bt.insert({1, 100});

    auto r1 = bt.search(3);
    auto r2 = bt.search(9);

    if(r1.has_value()) {
        cout << "found: " << r1.value() << endl;
    } else {
        cout << "not found" << endl;
    }

    if(r2.has_value()) {
        cout << "found: " << r2.value() << endl;
    } else {
        cout << "not found" << endl;
    }

    pager.read(page_id, buf);
    Cell* cells = reinterpret_cast<Cell*>(buf + PAGE_HEADER_SIZE);

    for(size_t i = 0; i < header->cell_count; i++) {
        cout << "key:" << cells[i].key << "| value: " << cells[i].value << endl;
    }
}

void test5() {
    std::remove("test.db");
    Pager pager;
    pager.open("test.db");
    uint32_t page_id = pager.allocate();
    uint8_t buf[PAGE_SIZE] = {};
    PageHeader* header = reinterpret_cast<PageHeader*>(buf);
    header->page_type = PAGE_TYPE::LEAF;
    header->cell_count = 0;
    header->free_space_offset = PAGE_HEADER_SIZE;
    pager.write(page_id, buf);

    BTree bt(&pager, page_id);
    for(uint32_t i = 0; i < MAX_CELLS + 10; i++) {
        bt.insert({i, i * 100});
        if(i % 100 == 0) {
            cout << "inserted: " << i << endl;
        }
    }

    for(uint32_t i = 0; i < MAX_CELLS + 10; i++) {
        auto r = bt.search(i);
        if(!r.has_value() || r.value() != i * 100) {
            cout << "FAIL: key " << i << endl;
        }
        if(i % 100 == 0) {
            cout << "searched: " << i << endl;
        }
    }
    cout << "TEST PASS" << endl;
}

void test6() {
    std::remove("test.db");
    Pager pager;
    pager.open("test.db");
    uint32_t page_id = pager.allocate();
    uint8_t buf[PAGE_SIZE] = {};
    PageHeader* header = reinterpret_cast<PageHeader*>(buf);
    header->page_type = PAGE_TYPE::LEAF;
    header->cell_count = 0;
    header->free_space_offset = PAGE_HEADER_SIZE;
    pager.write(page_id, buf);

    BTree bt(&pager, page_id);
    bt.insert({1, 100});
    bt.insert({3, 300});
    bt.insert({5, 500});
    bt.remove(3);

    auto r1 = bt.search(3);
    auto r2 = bt.search(1);
    auto r3 = bt.search(5);

    if(r1.has_value()) {
        cout << "found: " << r1.value() << endl;
    } else {
        cout << "not found" << endl;
    }

    if(r2.has_value()) {
        cout << "found: " << r2.value() << endl;
    } else {
        cout << "not found" << endl;
    }

    if(r3.has_value()) {
        cout << "found: " << r3.value() << endl;
    } else {
        cout << "not found" << endl;
    }
}

int main() {
    test6();
    return 0;
}
