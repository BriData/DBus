var React = require('react');
var $ = require('jquery');
var Reflux = require('reflux');
var Treebeard = require('react-treebeard').Treebeard;
var styles  = require('./styles');
var StyleRoot  = require('radium').StyleRoot;
var NodeViewerZkTree  = require('./node-view-zktree');
var decorators = require('react-treebeard').decorators;
var filters  = require('./filter');
var store = require('./zk-tree-store');
var FreeScrollBar = require('react-free-scrollbar');
var ShortCutSearch = require('./short-cut-search');
var utils = require('../utils');

decorators.Header = (props) => {
    const style = props.style;
    const iconClass = props.node.children ? 'glyphicon glyphicon-folder-open':'glyphicon glyphicon-list-alt';
    const iconStyle =  props.node.children ?  { marginRight: '5px', color:'#f5b041'}:{ marginRight: '5px', color:'#7f8c8d'};
    const existedStyle = props.node.existed ? {} : {color:'red'};
    const existedText =  props.node.existed ? '' : '【NOT Existed!】';
    return (
        <div style={style.base}>
            <div style={style.title}>
                <span className={iconClass} style={iconStyle}/>
                {props.node.name}
                <span style={existedStyle}>{existedText}</span>
            </div>
        </div>
    );

};

var ZkTree = React.createClass({
  mixins: [Reflux.listenTo(store, "_onStatusChange")],
  getInitialState: function() {
    var state = store.initState();
    return state;
  },
  _onStatusChange: function(state) {
    this.setState(state);
  },
  render: function() {
    var data=this.state.data||this.props.data||'';
      if(data){
      return (
        <StyleRoot>
        <div style={styles.searchBox}>
            <div className="input-group">
                <span className="input-group-addon glyphicon glyphicon-search"></span>
                <input type="text" className="form-control" placeholder="Search the tree..." onKeyUp={this.onFilterMouseUp}/>
            </div>
        </div>
        <div style={styles.shortCuts}>
            <ShortCutSearch  data= {JSON.stringify(data)} onChange={(shortCut)=>{this.onShortCutFilter(shortCut)}}/>
        </div>
        <div style={styles.scrolledContainer}>
              <FreeScrollBar>
                  <Treebeard data= {data}
                             onToggle={this.onToggle} decorators={decorators} style={styles}/>
              </FreeScrollBar>
        </div>
        <div style={styles.viewer.base}><NodeViewerZkTree node={this.state.cursor}  style={styles} onChange={(currentNode)=>{this.onNodeContentModified(currentNode)}}/></div>
        </StyleRoot>)
    }else{
      return (<div>Tree is Empty!</div>);
    }
  },
  onToggle:function(node, toggled){
    if(this.state.cursor){this.state.cursor.active = false;}
    node.active = true;
    if(node.children){ node.toggled = toggled; }
    this.setState({ cursor: node });
  },
  onFilterMouseUp:function(e){
    var filtered =this.props.data;
    const filter = e.target.value.trim();
    if(filter&&filter.length!=0){
      filtered = filters.filterTree(this.state.data?this.state.data:this.props.data, filter);
    }
    filtered = filters.expandFilteredNodes(filtered, filter);
    this.setState({data: filtered});
  },
  onShortCutFilter:function(shortCut){
    var shortCutObj=JSON.parse(shortCut);
    var filtered = filters.filterTree(this.props.data, shortCutObj.filterKey);
    // 仅显示指定路径下符合条件的。不严格符合需求描述，但够用，先这样。
    var pathArr=shortCutObj.parentPath.split("/");
    pathArr.forEach(function(path){
      if(path&&path!=''){
        filtered = filters.filterTree(filtered, path);
      }
    });
    filtered = filters.expandFilteredNodes(filtered, shortCutObj.filterKey);
    this.setState({data: filtered});
  } ,
  onNodeContentModified:function(node){
    this.state.cursor=node;
    store.trigger(this.state);
  }
});
module.exports = ZkTree;
