//
// Created by 19856 on 2021/4/21.
//

#include "../Header/Pair.h"

void Pair::writeToFile(std::ofstream &ofs) const {
    ofs.write((const char*)&key, 8);
    ofs.write((const char*)&offset, 4);
}
