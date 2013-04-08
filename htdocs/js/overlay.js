function Overlay(options) {
    
    options = jQuery.extend(true, { 
        containerId: 'container',
        delay: 200
    }, options || {});
    
    // copy all options to the object instance so they are available externally
    for (var opt in options) {
        this[opt] = options[opt];
    }
	
    this.overlayElement = jQuery('#' + this.containerId);
}

Overlay.prototype.show = function(html) {
    if (!this.overlayElement) {
        return;
    }
    
	var pthis = this;
	var overlayHtml = '<div class="overlay clearfix" id = "overlay">';
    overlayHtml += '<div class="background"></div>';
	overlayHtml += html;	
	overlayHtml += '</div>';
	pthis.overlayElement.append(overlayHtml);
}

Overlay.prototype.hide = function() {
    if (!this.overlayElement) {
        return;
    }
    jQuery('.overlay', this.overlayElement).remove();
}