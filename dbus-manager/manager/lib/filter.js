var ConfUtils = require('./utils/ConfUtils');
/**
 * Created by zhangyf on 16/9/19.
 */
var userName = ConfUtils.httpPort()
module.exports = {
    authorize: function (name, pwd, fn) {
        if (name.trim() == ConfUtils.userName && pwd.trim() == ConfUtils.password) {
            fn(null, {name: name});
        } else {
            fn({status: -1, message: "name or password invalid"});
        }
    }
}
