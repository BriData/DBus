module.exports = function (req, res, next) {
    // 判断是否已经登录
    if (!req.session.user) {
        if (req.path != '/login') {
            res.json({status:999, result:"INVALID_SESSION"});
        } else {
            next();
        }
    } else {
        next();
    }
}