(function($) {
  //备份jquery的ajax方法
  var _ajax = $.ajax;
  //重写jquery的ajax方法
  $.ajax = function(opt) {
    //备份opt中error和success方法
    var fn = {
      error: function(XMLHttpRequest, textStatus, errorThrown) {},
      success: function(data, textStatus) {}
    }
    if (opt.error) {
      fn.error = opt.error;
    }
    if (opt.success) {
      fn.success = opt.success;
    }

    //扩展增强处理
    var _opt = $.extend(opt, {
      error: function(XMLHttpRequest, textStatus, errorThrown) {
        //错误方法增强处理
        fn.error(XMLHttpRequest, textStatus, errorThrown);
      },
      success: function(data, textStatus) {
        if(data && data.status == 999 && data.result == 'INVALID_SESSION') {
            document.location.href = "#/login";
            return;
        }
        //成功回调方法增强处理
        fn.success(data, textStatus);
      }
    });
    return _ajax(_opt);
  };
})(jQuery);
