/* 
 * File:   Tile.cpp
 * Author: David V. Hill
 * 
 * Created on July 4, 2013, 12:51 AM
 */

#include "Tile.h"
#include <iostream>
#include <string>
#include <vector>
#include <cassert>

Tile::Tile(int x, int y, int tile_xsize, int tile_ysize, int band_count) {
    assert(tile_xsize > 0 && tile_ysize > 0 && band_count > 0);
    ulx = x;
    uly = y;
    tile_xsize = tile_xsize;
    tile_ysize = tile_ysize;
    bandcount = band_count;
}

Tile::Tile(const Tile& orig) {
}

Tile::~Tile() {
   
}

int Tile::getXSize() {
    return this->tile_xsize;
}

int Tile::getYSize() {
    return this->tile_ysize;
}

void Tile::setXSize(int size) {
    this->tile_xsize = size;
}

void Tile::setYSize(int size) {
    this->tile_ysize = size;
}

int Tile::getPixelXSize() {
    return 0;
}

int Tile::getPixelYSize() {
    return 0;
}

void Tile::setPixelXSize(int size) {

}

void Tile::setPixelYSize(int size) {

}

int Tile::getBandCount() {
    return this->bandcount;
}

int Tile::getPixelValue(int x, int y, int band) {
    return 0;
}

void Tile::setPixelValue(int x, int y, int band, int value) {

}

std::string Tile::getXForm() {
   return "xform";
}

void Tile::setXForm(std::string xform) {

}

