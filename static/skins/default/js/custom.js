/**
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

var URL = window.location,
    $BODY = jQuery('body'),
    $MENU_TOGGLE = jQuery('#menu_toggle'),
    $SIDEBAR_MENU = jQuery('#sidebar-menu'),
    $SIDEBAR_FOOTER = jQuery('.sidebar-footer'),
    $LEFT_COL = jQuery('.left_col'),
    $RIGHT_COL = jQuery('.right_col'),
    $NAV_MENU = jQuery('.nav_menu'),
    $FOOTER = jQuery('footer');

// Sidebar
jQuery(function () {

    // TODO: This is some kind of easy fix, maybe we can improve this
    var setContentHeight = function () {
        // reset height
        $RIGHT_COL.css('min-height', jQuery(window).height());

        var bodyHeight = $BODY.height(),
            leftColHeight = $LEFT_COL.eq(1).height() + $SIDEBAR_FOOTER.height(),
            contentHeight = bodyHeight < leftColHeight ? leftColHeight : bodyHeight;

        // normalize content
        contentHeight -= $NAV_MENU.height() + $FOOTER.height();

        $RIGHT_COL.css('min-height', contentHeight);
    };

    $SIDEBAR_MENU.find('a').on('click', function(ev) {
        var $li = jQuery(this).parent();

        if ($li.is('.active')) {
            $li.removeClass('active');
            jQuery('ul:first', $li).slideUp(function() {
                setContentHeight();
            });
        } else {
            // prevent closing menu if we are on child menu
            if (!$li.parent().is('.child_menu')) {
                $SIDEBAR_MENU.find('li').removeClass('active');
                $SIDEBAR_MENU.find('li ul').slideUp();
            }
            
            $li.addClass('active');

            jQuery('ul:first', $li).slideDown(function() {
                setContentHeight();
            });
        }
    });

    // toggle small or large menu
    $MENU_TOGGLE.on('click', function() {
        if ($BODY.hasClass('nav-md')) {
            $BODY.removeClass('nav-md').addClass('nav-sm');
            $LEFT_COL.removeClass('scroll-view').removeAttr('style');

            if ($SIDEBAR_MENU.find('li').hasClass('active')) {
                $SIDEBAR_MENU.find('li.active').addClass('active-sm').removeClass('active');
            }
        } else {
            $BODY.removeClass('nav-sm').addClass('nav-md');

            if ($SIDEBAR_MENU.find('li').hasClass('active-sm')) {
                $SIDEBAR_MENU.find('li.active-sm').addClass('active').removeClass('active-sm');
            }
        }

        setContentHeight();
    });

    // check active menu
    $SIDEBAR_MENU.find('a[href="' + URL + '"]').parent('li').addClass('current-page');

    $SIDEBAR_MENU.find('a').filter(function () {
        console.log( '->'+this.href );
		if( this.href.includes( 'dispSvcrmAdmin' ) )
			return URL.href.includes( 'dispSvcrmAdmin');
		else if( this.href.includes( 'dispSvpromotionAdmin' ) )
			return URL.href.includes( 'dispSvpromotionAdmin');
		else if( this.href.includes( 'dispSvorderAdmin' ) )
			return URL.href.includes( 'dispSvorderAdmin');
		else if( this.href.includes( 'dispSvitemAdmin' ) )
			return URL.href.includes( 'dispSvitemAdmin');
		else if( this.href.includes( 'dispSvcartAdmin' ) )
			return URL.href.includes( 'dispSvcartAdmin');
		else if( this.href.includes( 'dispSvpgAdmin' ) )
			return URL.href.includes( 'dispSvpgAdmin');
		else if( this.href.includes( 'dispSvestudioAdmin' ) )
			return URL.href.includes( 'dispSvestudioAdmin');

		//return this.href == URL;
    }).parent('li').addClass('current-page').parents('ul').slideDown(function() {
        setContentHeight();
    }).parent().addClass('active');

    // recompute content when resizing
    jQuery(window).smartresize(function(){  
        setContentHeight();
    });

});

// Panel toolbox
jQuery(function () {
    jQuery('.collapse-link').on('click', function() {
        var $BOX_PANEL = jQuery(this).closest('.x_panel'),
            $ICON = jQuery(this).find('i'),
            $BOX_CONTENT = $BOX_PANEL.find('.x_content');
        
        // fix for some div with hardcoded fix class
        if ($BOX_PANEL.attr('style')) {
            $BOX_CONTENT.slideToggle(200, function(){
                $BOX_PANEL.removeAttr('style');
            });
        } else {
            $BOX_CONTENT.slideToggle(200); 
            $BOX_PANEL.css('height', 'auto');  
        }

        $ICON.toggleClass('fa-chevron-up fa-chevron-down');
    });

    jQuery('.close-link').click(function () {
        var $BOX_PANEL = jQuery(this).closest('.x_panel');

        $BOX_PANEL.remove();
    });
});

// Tooltip
jQuery(function () {
    jQuery('[data-toggle="tooltip"]').tooltip();
});

// Progressbar
if (jQuery(".progress .progress-bar")[0]) {
    jQuery('.progress .progress-bar').progressbar(); // bootstrap 3
}

// Switchery
if (jQuery(".js-switch")[0]) {
    var elems = Array.prototype.slice.call(document.querySelectorAll('.js-switch'));
    elems.forEach(function (html) {
        var switchery = new Switchery(html, {
            color: '#26B99A'
        });
    });
}

// iCheck
if (jQuery("input.flat")[0]) {
    jQuery(document).ready(function () {
        jQuery('input.flat').iCheck({
            checkboxClass: 'icheckbox_flat-green',
            radioClass: 'iradio_flat-green'
        });
    });
}

// Starrr
var __slice = [].slice;

(function ($, window) {
    var Starrr;

    Starrr = (function () {
        Starrr.prototype.defaults = {
            rating: void 0,
            numStars: 5,
            change: function (e, value) {
            }
        };

        function Starrr($el, options) {
            var i, _, _ref,
                    _this = this;

            this.options = $.extend({}, this.defaults, options);
            this.$el = $el;
            _ref = this.defaults;
            for (i in _ref) {
                _ = _ref[i];
                if (this.$el.data(i) !== null) {
                    this.options[i] = this.$el.data(i);
                }
            }
            this.createStars();
            this.syncRating();
            this.$el.on('mouseover.starrr', 'span', function (e) {
                return _this.syncRating(_this.$el.find('span').index(e.currentTarget) + 1);
            });
            this.$el.on('mouseout.starrr', function () {
                return _this.syncRating();
            });
            this.$el.on('click.starrr', 'span', function (e) {
                return _this.setRating(_this.$el.find('span').index(e.currentTarget) + 1);
            });
            this.$el.on('starrr:change', this.options.change);
        }

        Starrr.prototype.createStars = function () {
            var _i, _ref, _results;

            _results = [];
            for (_i = 1, _ref = this.options.numStars; 1 <= _ref ? _i <= _ref : _i >= _ref; 1 <= _ref ? _i++ : _i--) {
                _results.push(this.$el.append("<span class='glyphicon .glyphicon-star-empty'></span>"));
            }
            return _results;
        };

        Starrr.prototype.setRating = function (rating) {
            if (this.options.rating === rating) {
                rating = void 0;
            }
            this.options.rating = rating;
            this.syncRating();
            return this.$el.trigger('starrr:change', rating);
        };

        Starrr.prototype.syncRating = function (rating) {
            var i, _i, _j, _ref;

            rating || (rating = this.options.rating);
            if (rating) {
                for (i = _i = 0, _ref = rating - 1; 0 <= _ref ? _i <= _ref : _i >= _ref; i = 0 <= _ref ? ++_i : --_i) {
                    this.$el.find('span').eq(i).removeClass('glyphicon-star-empty').addClass('glyphicon-star');
                }
            }
            if (rating && rating < 5) {
                for (i = _j = rating; rating <= 4 ? _j <= 4 : _j >= 4; i = rating <= 4 ? ++_j : --_j) {
                    this.$el.find('span').eq(i).removeClass('glyphicon-star').addClass('glyphicon-star-empty');
                }
            }
            if (!rating) {
                return this.$el.find('span').removeClass('glyphicon-star').addClass('glyphicon-star-empty');
            }
        };

        return Starrr;

    })();
    return $.fn.extend({
        starrr: function () {
            var args, option;

            option = arguments[0], args = 2 <= arguments.length ? __slice.call(arguments, 1) : [];
            return this.each(function () {
                var data;

                data = jQuery(this).data('star-rating');
                if (!data) {
                    jQuery(this).data('star-rating', (data = new Starrr(jQuery(this), option)));
                }
                if (typeof option === 'string') {
                    return data[option].apply(data, args);
                }
            });
        }
    });
})(window.jQuery, window);

jQuery(function () {
    return jQuery(".starrr").starrr();
});

jQuery(document).ready(function () {

    jQuery('#stars').on('starrr:change', function (e, value) {
        jQuery('#count').html(value);
    });


    jQuery('#stars-existing').on('starrr:change', function (e, value) {
        jQuery('#count-existing').html(value);
    });

});

// Table
jQuery('table input').on('ifChecked', function () {
    check_state = '';
    jQuery(this).parent().parent().parent().addClass('selected');
    countChecked();
});
jQuery('table input').on('ifUnchecked', function () {
    check_state = '';
    jQuery(this).parent().parent().parent().removeClass('selected');
    countChecked();
});

var check_state = '';
jQuery('.bulk_action input').on('ifChecked', function () {
    check_state = '';
    jQuery(this).parent().parent().parent().addClass('selected');
    countChecked();
});
jQuery('.bulk_action input').on('ifUnchecked', function () {
    check_state = '';
    jQuery(this).parent().parent().parent().removeClass('selected');
    countChecked();
});
jQuery('.bulk_action input#check-all').on('ifChecked', function () {
    check_state = 'check_all';
    countChecked();
});
jQuery('.bulk_action input#check-all').on('ifUnchecked', function () {
    check_state = 'uncheck_all';
    countChecked();
});

function countChecked() {
    if (check_state == 'check_all') {
        jQuery(".bulk_action input[name='table_records']").iCheck('check');
    }
    if (check_state == 'uncheck_all') {
        jQuery(".bulk_action input[name='table_records']").iCheck('uncheck');
    }
    var n = jQuery(".bulk_action input[name='table_records']:checked").length;
    if (n > 0) {
        jQuery('.column-title').hide();
        jQuery('.bulk-actions').show();
        jQuery('.action-cnt').html(n + ' Records Selected');
    } else {
        jQuery('.column-title').show();
        jQuery('.bulk-actions').hide();
    }
}

// Accordion
jQuery(function () {
    jQuery(".expand").on("click", function () {
        jQuery(this).next().slideToggle(200);
        $expand = jQuery(this).find(">:first-child");

        if ($expand.text() == "+") {
            $expand.text("-");
        } else {
            $expand.text("+");
        }
    });
});


// NProgress
if (typeof NProgress != 'undefined') {
    jQuery(document).ready(function () {
        NProgress.start();
    });

    jQuery(window).load(function () {
        NProgress.done();
    });
}

/**
 * Resize function without multiple trigger
 * 
 * Usage:
 * jQuery(window).smartresize(function(){  
 *     // code here
 * });
 */
(function($,sr){
    // debouncing function from John Hann
    // http://unscriptable.com/index.php/2009/03/20/debouncing-javascript-methods/
    var debounce = function (func, threshold, execAsap) {
      var timeout;

        return function debounced () {
            var obj = this, args = arguments;
            function delayed () {
                if (!execAsap)
                    func.apply(obj, args);
                timeout = null; 
            }

            if (timeout)
                clearTimeout(timeout);
            else if (execAsap)
                func.apply(obj, args);

            timeout = setTimeout(delayed, threshold || 100); 
        };
    };

    // smartresize 
    jQuery.fn[sr] = function(fn){  return fn ? this.bind('resize', debounce(fn)) : this.trigger(sr); };

})(jQuery,'smartresize');