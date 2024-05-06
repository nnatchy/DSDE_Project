function Animation(img_id, slider_id, frames) {
    this.img_id = img_id;
    this.slider_id = slider_id;
    this.frames = frames.map(function(src) {
        var img = new Image();
        img.src = src;
        return img;
    });
    this.current_frame = 0;
    this.interval = 100; // Default interval between frames in milliseconds
    this.timer = null;
    this.direction = 1; // 1 for forward, -1 for reverse

    var slider = document.getElementById(this.slider_id);
    slider.max = this.frames.length - 1;
    slider.value = 0;
    slider.oninput = () => { this.set_frame(parseInt(slider.value)); };

    if (navigator.userAgent.indexOf("MSIE") !== -1 || navigator.appVersion.indexOf("Trident/") > 0) {
        // Adjust for Internet Explorer
        slider.onchange = slider.oninput;
        slider.oninput = null;
    }

    this.set_frame(this.current_frame);
}

Animation.prototype.set_frame = function(frame) {
    this.current_frame = frame;
    var img = document.getElementById(this.img_id);
    img.src = this.frames[this.current_frame].src;
    document.getElementById(this.slider_id).value = this.current_frame;
};

Animation.prototype.next_frame = function() {
    if (this.current_frame < this.frames.length - 1) {
        this.set_frame(this.current_frame + 1);
    } else {
        this.pause_animation(); // Optional: stop at the last frame
    }
};

Animation.prototype.previous_frame = function() {
    if (this.current_frame > 0) {
        this.set_frame(this.current_frame - 1);
    }
};

Animation.prototype.play_animation = function() {
    this.pause_animation();
    this.direction = 1;
    this.timer = setInterval(() => { this.next_frame(); }, this.interval);
};

Animation.prototype.reverse_animation = function() {
    this.pause_animation();
    this.direction = -1;
    this.timer = setInterval(() => { this.previous_frame(); }, this.interval);
};

Animation.prototype.pause_animation = function() {
    clearInterval(this.timer);
    this.timer = null;
};

Animation.prototype.slower = function() {
    this.interval /= 0.7;
    if (this.timer) {
        this.pause_animation();
        this.play_animation();
    }
};

Animation.prototype.faster = function() {
    this.interval *= 0.7;
    if (this.timer) {
        this.pause_animation();
        this.play_animation();
    }
};
