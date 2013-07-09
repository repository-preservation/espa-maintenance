/* 
 * File:   run.cpp
 * Author: dhill
 *
 * Created on July 8, 2013, 2:17 PM
 */

#include <cstdlib>
#include <iostream>
#include "Tile.h"


using namespace std;

/*
 * 
 */
int main(int argc, char** argv) {
    cout << "Starting up" << endl;
    cout << "Creating tile" << endl;
    Tile *t = new Tile(10, 20, 2500, 2500, 8);
    t->setXSize(2500);
    t->setYSize(2500);
    cout << "x,y is " << t->getXSize() << ',' << t->getYSize() << endl;

    cout << "Tile XForm is:"<< t->getXForm() << endl;

    cout << "Tile Bandcount is:"<< t->getBandCount() << endl;
    delete(t);
    
    return 0;
}

