#include "btree.h"
#include "src/page.h"
#include <cstring>
#include <optional>

using namespace ysql;

uint32_t BTree::find_leaf(uint32_t key) {
    uint8_t buf[PAGE_SIZE] = {};
    pager->read(root_page_id, buf);
    PageHeader* header = reinterpret_cast<PageHeader*>(buf);
    if(header->page_type == PAGE_TYPE::LEAF) {
        return root_page_id;
    }
    InternalHeader* iheader = reinterpret_cast<InternalHeader*>(buf);
    InternalCell* icells = reinterpret_cast<InternalCell*>(buf + sizeof(InternalHeader));

    for(size_t i = 0; i < iheader->base.cell_count; i++) {
        if(key < icells[i].key) {
            if(i == 0) {
                return iheader->left_child;
            } else {
                return icells[i - 1].child_page_id;
            }
        }     
    }
    return icells[iheader->base.cell_count - 1].child_page_id;
}

void BTree::insert(const Cell& cell) {
    uint32_t leaf_id = find_leaf(cell.key);
    uint8_t buf[PAGE_SIZE] = {};
    pager->read(leaf_id, buf);
    PageHeader* header = reinterpret_cast<PageHeader*>(buf);
    if(header->cell_count == MAX_CELLS) {
        split(leaf_id);
        leaf_id = find_leaf(cell.key);
        pager->read(leaf_id, buf);
        header = reinterpret_cast<PageHeader*>(buf);
    }

    Cell* cells = reinterpret_cast<Cell*>(buf + PAGE_HEADER_SIZE);
    
    size_t i;
    for(i = header->cell_count; i > 0; i--) {
        if(cells[i - 1].key > cell.key) {
            cells[i] = cells[i - 1];
        } else {
            cells[i] = cell;
            break;
        }
    }
    if(i == 0) {
        cells[0] = cell;
    }
    header->cell_count++;
    pager->write(leaf_id, buf);

}

std::optional<uint32_t> BTree::search(const uint32_t& key) {
    uint32_t leaf_id = find_leaf(key);
    uint8_t buf[PAGE_SIZE] = {};
    pager->read(leaf_id, buf);

    Cell* cells = reinterpret_cast<Cell*>(buf + PAGE_HEADER_SIZE);
    PageHeader* header = reinterpret_cast<PageHeader*>(buf);
    for(size_t i = 0; i < header->cell_count; i++) {
        if(cells[i].key == key) {
            return cells[i].value;
        }
        if(cells[i].key > key) {
            return std::nullopt;
        } 
    }
    return std::nullopt;
}

void BTree::split(uint32_t page_id) {
    uint8_t buf[PAGE_SIZE] = {};
    pager->read(page_id, buf);
    
    PageHeader* header = reinterpret_cast<PageHeader*>(buf);
    Cell* cell = reinterpret_cast<Cell*>(buf + PAGE_HEADER_SIZE);
    uint16_t mid = header->cell_count / 2;
    uint32_t split_key = cell[mid].key;

    uint8_t nbuf[PAGE_SIZE] = {};
    PageHeader* nheader = reinterpret_cast<PageHeader*>(nbuf);
    nheader->page_type = PAGE_TYPE::LEAF;
    nheader->cell_count = header->cell_count - mid;
    Cell* ncell = reinterpret_cast<Cell*>(nbuf + PAGE_HEADER_SIZE);

    std::memcpy(ncell, cell + mid, sizeof(Cell) * (header->cell_count - mid));
    uint32_t new_page_id = pager->allocate();
    pager->write(new_page_id, nbuf);

    header->cell_count = mid;
    pager->write(page_id, buf);

    uint32_t internal_id = pager->allocate();

    uint8_t ibuf[PAGE_SIZE] = {};
    InternalHeader* iheader = reinterpret_cast<InternalHeader*>(ibuf);
    iheader->base.page_type = PAGE_TYPE::INTERNAL;
    iheader->base.cell_count = 1;
    iheader->left_child = page_id;

    InternalCell* icells = reinterpret_cast<InternalCell*>(ibuf + sizeof(InternalHeader));
    icells[0].key = split_key;
    icells[0].child_page_id = new_page_id;

    pager->write(internal_id, ibuf);
    root_page_id = internal_id;
}

void BTree::remove(const uint32_t& key) {
    uint32_t leaf_id = find_leaf(key);
    uint8_t buf[PAGE_SIZE] = {};
    pager->read(leaf_id, buf);
    PageHeader* header = reinterpret_cast<PageHeader*>(buf);
    Cell* cells = reinterpret_cast<Cell*>(buf + PAGE_HEADER_SIZE);
    for(size_t i = 0; i < header->cell_count; i++) {
        if(cells[i].key == key) {
            for(size_t j = i; j < header->cell_count - 1; j++) {
                cells[j] = cells[j + 1];
            }
            header->cell_count--;
            pager->write(leaf_id, buf);
            return;
        }
    }
}
