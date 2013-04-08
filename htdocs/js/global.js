function toProperCase(str) {
    return str.replace(/\w\S*/g, function(txt){
        return txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase();
    });
}

function countMembers(obj) {
    var i = 0;
    if (!obj) {
        return i;
    }
    for (var key in obj) {
        i++;
    }
    return i;
}

function getMemberByIndex(obj, index) {
    var member;
    if (!obj) {
        return member;
    }
    var i = 0;
    for (var key in obj) {
        if (i == index) {
            member = key;
            break;
        }
        i++;
    }
    return member;
}

function getMemberIndex(obj, member) {
    var i = 0;
    if (!obj) {
        return i;
    }
    for (var key in obj) {
        if (key == member) {
            break;
        }
        i++;
    }
    return i;
}

function addCommas(value) {
    value += '';
    var x = value.split('.');
    var x1 = x[0];
    var x2 = x.length > 1 ? '.' + x[1] : '';
    var rgx = /(\d+)(\d{3})/;
    while (rgx.test(x1)) {
        x1 = x1.replace(rgx, '$1' + ',' + '$2');
    }
    return x1 + x2;
}

function isNumeric(obj) {
    return typeof obj === 'number' && isFinite(obj);
}

function formatPercent(num) {
    if (num) {
        return parseFloat(num * 100).toFixed(2) + ' %';
    }
}

function zeroPad(number, length) {
    var result = number.toString();
    var decIndex = result.indexOf('.');
    var pad = length - (decIndex > 0 ? decIndex : result.length);
    while(pad > 0) {
        result = '0' + result;
        pad--;
    }
    return result;
}

function moveIntoVisible(elem, parent) {
    
    // elem must be a jQuery object
    if ((!elem) || (elem.length <= 0)) {
        return;
    }
    
    // determine what to use as the parent window
    var win = jQuery('#TB_window');
    if (win.length <= 0) {
        win = jQuery(window);
    } else {
        // use the container for the parent
        win = parent.parent();
    };
    
    // calculate the position of the scroll view
    var docViewTop = win.scrollTop();
    var docViewBottom = docViewTop + win.height();
    var docViewLeft = win.scrollLeft();
    var docViewRight = docViewLeft + win.width();
    
    // calculate the position of the element
    var position = elem.position();
    var elemTop = position.top;
    var elemBottom = elemTop + elem.height();
    var elemLeft = position.left;
    var elemRight = elemLeft + elem.width();
    
    // determine where the element should be positioned so it's visible
    var newElemTop = elemTop;
    var newElemLeft = elemLeft;
    if (elemBottom > docViewBottom) {
        newElemTop = docViewBottom - elem.height() - ((parent) ? parent.offset().top : 0);
    } else if (elemTop < docViewTop) {
        newElemTop = docViewTop;
    }
    if (elemRight > docViewRight) {
        newElemLeft = docViewRight - elem.width() - ((parent) ? parent.offset().left : 0);
    } else if (elemRight < docViewLeft) {
        newElemLeft = docViewLeft;
    }
    
    // reposition the element
    elem.css({
        top : newElemTop + 'px',
        left: newElemLeft + 'px'
    });
    
}