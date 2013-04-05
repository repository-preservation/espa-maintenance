function Progress(options) {
    
    options = jQuery.extend(true, { 
        containerId: 'container',
        delay: 200
    }, options || {});
    
    // copy all options to the object instance so they are available externally
    for (var opt in options) {
        this[opt] = options[opt];
    }
    
    this.progressTimer = null;   
    this.progressElement = jQuery('#' + this.containerId);
}

Progress.prototype.show = function() {
    show(this.delay);
}

Progress.prototype.show = function(delay) {
    if (!this.progressElement) {
        return;
    }
    var pthis = this;
    this.progressTimer = setTimeout(function() {
        var progressHtml = '<div class="progress clearfix">';
        progressHtml += '<div class="background"></div>';
        progressHtml += '<div class="spinner"></div></div>';
        pthis.progressElement.append(progressHtml);
    }, this.delay);
}

Progress.prototype.hide = function() {
    if (!this.progressElement) {
        return;
    }
    jQuery('.progress', this.progressElement).remove();
    clearTimeout(this.progressTimer);
}