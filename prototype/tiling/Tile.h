/* 
 * File:   Tile.h
 * Author: David V. Hill
 *
 * Created on July 4, 2013, 12:51 AM
 */

#ifndef TILE_H
#define	TILE_H
#include <string>
#include <vector>

class Tile {
private:
    int tile_xsize;
    int tile_ysize;
    int bandcount;
    int pixel_xsize, pixel_ysize;
    int ulx, uly;
    //x, y, band
    std::vector<std::vector<std::vector<int> > > data;
    std::string xform;

public:
    Tile(int x, int y, int tilex, int tiley, int band_count);
    Tile(const Tile& orig);
    virtual ~Tile();
    int getXSize();
    int getYSize();
    void setXSize(int xsize);
    void setYSize(int ysize);
    int getPixelXSize();
    int getPixelYSize();
    void setPixelXSize(int xsize);
    void setPixelYSize(int ysize);
    int getBandCount();
    void setPixelValue(int x, int y, int band, int value);
    int getPixelValue(int x, int y, int band);
    std::string getXForm();
    void setXForm(std::string xform);    
};

#endif	/* TILE_H */

