#include "btree.h"
#include "src/page.h"
#include <cstdio>
#include <iostream>
#include <cstring>
#include <optional>

using namespace ysql;

uint32_t BTree::find_leaf(uint32_t key, std::vector<PathEntry>& path) {
    path.clear();

    uint32_t current = root_page_id;
    int depth = 0;
    while(true) {
        depth++;
        uint8_t* buf = buffer_pool->get_page(current);
        PageHeader* header = reinterpret_cast<PageHeader*>(buf);
        if(header->page_type == PAGE_TYPE::LEAF) {
            buffer_pool->unpin_page(current);
            return current;
        }
        InternalHeader* iheader = reinterpret_cast<InternalHeader*>(buf);
        InternalCell* icells = reinterpret_cast<InternalCell*>(buf + sizeof(InternalHeader));

        uint32_t child_index = iheader->base.cell_count;
        for(uint32_t i = 0; i < iheader->base.cell_count; i++) {
            if(key < icells[i].key) {
                child_index = i;
                break;
            }     
        }
         
        uint32_t next_page;
        if(child_index == 0) {
            next_page = iheader->left_child_id;
        } else {
            next_page = icells[child_index - 1].child_page_id;
        }

        path.push_back({current, child_index});
        buffer_pool->unpin_page(current);
        current = next_page;
    }
    std::cerr << "find_leaf depth=" << depth << std::endl;
}

uint32_t BTree::find_leaf(uint32_t key) {
    std::vector<PathEntry> dummy;
    return find_leaf(key, dummy);
}

void BTree::insert(const LeafCell& cell) {
    std::vector<PathEntry> path;
    uint32_t leaf_id = find_leaf(cell.key, path);
    uint8_t* buf = buffer_pool->get_page(leaf_id);
    LeafHeader* header = reinterpret_cast<LeafHeader*>(buf);
    if(header->base.cell_count == MAX_LEAF_CELLS) {
        buffer_pool->unpin_page(leaf_id);
        split(leaf_id, path);
        leaf_id = find_leaf(cell.key, path);
        buf = buffer_pool->get_page(leaf_id);
        header = reinterpret_cast<LeafHeader*>(buf);
    }

    LeafCell* cells = reinterpret_cast<LeafCell*>(buf + sizeof(LeafHeader));
    
    size_t i;
    for(i = header->base.cell_count; i > 0; i--) {
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
    header->base.cell_count++;
    buffer_pool->make_dirty(leaf_id);
    buffer_pool->unpin_page(leaf_id);
}

void BTree::insert_into_parent(uint32_t parent_id, uint32_t child_index, uint32_t split_key, uint32_t new_page_id, const std::vector<PathEntry>& path) {
    uint8_t* buf = buffer_pool->get_page(parent_id);
    InternalHeader* iheader = reinterpret_cast<InternalHeader*>(buf);
    InternalCell* icells = reinterpret_cast<InternalCell*>(buf + sizeof(InternalHeader));

    memmove(&icells[child_index + 1], &icells[child_index], (iheader->base.cell_count - child_index) * sizeof(InternalCell));
    icells[child_index].key = split_key;
    icells[child_index].child_page_id = new_page_id;
    iheader->base.cell_count += 1;
    buffer_pool->make_dirty(parent_id);

    bool need_split = (iheader->base.cell_count >= MAX_INTERNAL_CELLS);
    buffer_pool->unpin_page(parent_id);

    if(need_split) {
        std::vector<PathEntry> parent_path = path;
        if(!parent_path.empty()) parent_path.pop_back();
        split_internal(parent_id, parent_path);
    }
}

std::optional<uint32_t> BTree::search(const uint32_t& key) {
    uint32_t leaf_id = find_leaf(key);
    uint8_t* buf = buffer_pool->get_page(leaf_id);

    LeafCell* cells = reinterpret_cast<LeafCell*>(buf + sizeof(LeafHeader));
    LeafHeader* header = reinterpret_cast<LeafHeader*>(buf);

    std::optional<uint32_t> result = std::nullopt;
    for(size_t i = 0; i < header->base.cell_count; i++) {
        if(cells[i].key == key) {
            result = cells[i].value;
            break; 
        }
        if(cells[i].key > key) {
            break;
        } 
    }

    buffer_pool->unpin_page(leaf_id);
    return result;
}

void BTree::split(uint32_t page_id, const std::vector<PathEntry>& path) {
    uint8_t* buf = buffer_pool->get_page(page_id);
    
    LeafHeader* header = reinterpret_cast<LeafHeader*>(buf);
    LeafCell* cell = reinterpret_cast<LeafCell*>(buf + sizeof(LeafHeader));
    uint16_t mid = header->base.cell_count / 2;
    uint32_t split_key = cell[mid].key;

    uint32_t new_page_id = pager->allocate();
    uint8_t* nbuf = buffer_pool->get_page(new_page_id);
    LeafHeader* nheader = reinterpret_cast<LeafHeader*>(nbuf);
    nheader->base.page_type = PAGE_TYPE::LEAF;
    nheader->base.cell_count = header->base.cell_count - mid;
    nheader->next_leaf = header->next_leaf;
    header->next_leaf = new_page_id;
    LeafCell* ncell = reinterpret_cast<LeafCell*>(nbuf + sizeof(LeafHeader));
    std::memcpy(ncell, cell + mid, sizeof(LeafCell) * (header->base.cell_count - mid));
    header->base.cell_count = mid;
    buffer_pool->make_dirty(new_page_id);
    buffer_pool->make_dirty(page_id);

    buffer_pool->unpin_page(new_page_id);
    buffer_pool->unpin_page(page_id);

    if(path.empty()) {
        uint32_t internal_id = pager->allocate();
        uint8_t* ibuf = buffer_pool->get_page(internal_id);
        InternalHeader* iheader = reinterpret_cast<InternalHeader*>(ibuf);
        iheader->base.page_type = PAGE_TYPE::INTERNAL;
        iheader->base.cell_count = 1;
        iheader->left_child_id = page_id;

        InternalCell* icells = reinterpret_cast<InternalCell*>(ibuf + sizeof(InternalHeader));
        icells[0].key = split_key;
        icells[0].child_page_id = new_page_id;

        buffer_pool->make_dirty(internal_id);
        root_page_id = internal_id;
        buffer_pool->unpin_page(internal_id);
    } else {
        const PathEntry& parent = path.back();
        insert_into_parent(parent.page_id, parent.child_index, split_key, new_page_id, path);
    }
}

void BTree::split_internal(uint32_t page_id, const std::vector<PathEntry>& path) {
    uint8_t* ibuf = buffer_pool->get_page(page_id);

    InternalHeader* iheader = reinterpret_cast<InternalHeader*>(ibuf);
    InternalCell* icells = reinterpret_cast<InternalCell*>(ibuf + sizeof(InternalHeader));
    uint16_t mid = iheader->base.cell_count / 2;
    uint32_t split_key = icells[mid].key;

    uint32_t new_page_id = pager->allocate();
    uint8_t* inbuf = buffer_pool->get_page(new_page_id);
    InternalHeader* inheader = reinterpret_cast<InternalHeader*>(inbuf);
    inheader->base.page_type = PAGE_TYPE::INTERNAL;
    inheader->left_child_id = icells[mid].child_page_id;

    InternalCell* incells = reinterpret_cast<InternalCell*>(inbuf + sizeof(InternalHeader));
    std::memcpy(incells, &icells[mid + 1], sizeof(InternalCell) * (iheader->base.cell_count - mid - 1));
    inheader->base.cell_count = iheader->base.cell_count - mid - 1;
    iheader->base.cell_count = mid;

    buffer_pool->make_dirty(new_page_id);
    buffer_pool->make_dirty(page_id);

    buffer_pool->unpin_page(new_page_id);
    buffer_pool->unpin_page(page_id);

    if(path.empty()) {
        uint32_t internal_id = pager->allocate();
        uint8_t* ibuf2 = buffer_pool->get_page(internal_id);
        InternalHeader* iheader2 = reinterpret_cast<InternalHeader*>(ibuf2);
        iheader2->base.page_type = PAGE_TYPE::INTERNAL;
        iheader2->base.cell_count = 1;
        iheader2->left_child_id = page_id;

        InternalCell* icells2 = reinterpret_cast<InternalCell*>(ibuf2 + sizeof(InternalHeader));
        icells2[0].key = split_key;
        icells2[0].child_page_id = new_page_id;

        buffer_pool->make_dirty(internal_id);
        root_page_id = internal_id;
        buffer_pool->unpin_page(internal_id);
    } else {
        insert_into_parent(path.back().page_id, path.back().child_index, split_key, new_page_id, path);
    }
}

void BTree::remove(const uint32_t& key) {
    uint32_t leaf_id = find_leaf(key);
    uint8_t* buf = buffer_pool->get_page(leaf_id);

    LeafHeader* header = reinterpret_cast<LeafHeader*>(buf);
    LeafCell* cells = reinterpret_cast<LeafCell*>(buf + sizeof(LeafHeader));

    for(size_t i = 0; i < header->base.cell_count; i++) {
        if(cells[i].key == key) {
            for(size_t j = i; j < header->base.cell_count - 1; j++) {
                cells[j] = cells[j + 1];
            }
            header->base.cell_count--;
            buffer_pool->make_dirty(leaf_id);
            break;
        }
    }
    buffer_pool->unpin_page(leaf_id);
}
