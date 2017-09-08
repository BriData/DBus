var React = require('react');
var styles  = require('./styles');
var ShortCutSearch  = React.createClass({
  shortcutOnlick:function (e) {
    this.props.onChange(e.currentTarget.getAttribute('data-shortCut'));
  },
    render:function () {
      var self=this;
      var shortCuts=[];
      var shortCutsList = [];
      if(this.props.data) {
        try{
          var treeObj = JSON.parse(this.props.data);
          if (treeObj.children) {
            var children = treeObj.children;
            for (var i = 0; i < children.length; i++) {
              if (children[i].name == 'DBus') {
                shortCuts = JSON.parse(children[i].content).shortCuts;
                break;
              }
            }
          }
        }catch(e) {

        }
      }

      shortCuts.forEach(function(shortCut,i){
            shortCutsList.push(
            <span data-shortCut={JSON.stringify(shortCut)} onClick={self.shortcutOnlick}  key={i}> {shortCut.label}</span>
            );
              shortCutsList.push(" | ");
          });

        return (<div>{shortCutsList}</div>);
    }
});

module.exports = ShortCutSearch;
