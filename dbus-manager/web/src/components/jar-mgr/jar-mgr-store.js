var Reflux = require('reflux');
var React = require('react');
var $ = require('jquery')
var utils = require('../common/utils');
import ReactDOM from 'react-dom';
import DropzoneComponent from 'react-dropzone-component';
var ReactDOMServer = require('react-dom/server');
var actions = Reflux.createActions(['initialLoad','openDialog','closeDialog']);

var store = Reflux.createStore({
    //监听所有的actions
    listenables: [actions],
    state: {
        jarList: [],
        dialog: {
            show: false,
            content:"",
        },
        id:-1,
        componentConfig: {
            iconFiletypes: ['.jar','.png','.txt','.zip','.rar'],
            showFiletypeIcon: true,
            postUrl: '/dbus/jarManager/upload'},
        djsConfig: {
            clickable: true,
            addRemoveLinks: false,
            previewsContainer: null,
            createImageThumbnails:true,
            maxFilesize: 100,
            thumbnailWidth: null,
            thumbnailHeight: null,
            resize: true,
            previewTemplate: ReactDOMServer.renderToStaticMarkup(
                <div className="dz-preview dz-file-preview">
                  <div className="dz-details">
                    <div className="dz-filename"><span data-dz-name="true"></span></div>
                    <img data-dz-thumbnail="true" />
                  </div>
                  <div className="dz-progress"><span className="dz-upload" data-dz-uploadprogress="true"></span></div>
                  <div className="dz-success-mark glyphicon glyphicon-cloud-upload"></div>
                  <div className="dz-error-mark glyphicon glyphicon-remove"></div>
                  <div className="dz-error-message"><span data-dz-errormessage="true"></span></div>
                </div>
            )
        },
        eventHandlers: null
    },
    initState: function(r) {
        this.state.eventHandlers =  {
            init: function() {
                console.log("init: " + r);
            },
            drop: function(e){
                console.log("init: " + JSON.stringify(r));
                console.log("drop: " + JSON.stringify(e));
            },
            error: function(err) {
                console.log("err: " + JSON.stringify(err));
                alert("上传失败！");
            },
            addedfile: function(file) {
                console.log("file: " + JSON.stringify(file));
            },
            dragstart: function(e){
                console.log("dragstart: " + JSON.stringify(e) );
            },
            dragend: function(e){
                console.log("dragend: " + JSON.stringify(e) );
            },
            dragenter: function(e){
                console.log("dragend: " + JSON.stringify(e) );
            },
            processing: function(e){
                console.log("dragenter: " + JSON.stringify(e));
            },
            dragover: function(e){
                console.log("dragover: " + JSON.stringify(e));
            },
            dragleave: function(e){
                console.log("dragleave: " + JSON.stringify(e));
            },
            removedfile: function(e){
                console.log("removedfile: " + JSON.stringify(e));
            },
            sending: function(e){
                console.log("sending: " + JSON.stringify(e));
            },
            uploadprogress: function(e){
                console.log("uploadprogress: " + JSON.stringify(e));
            },
            canceled: function(e){
                console.log("canceled: " + JSON.stringify(e));
            },
            thumbnail: function(e){
                console.log("thumbnail: " + JSON.stringify(e));
            },
            maxfilesreached: function(e){
                console.log("maxfilesreached: " + JSON.stringify(e));
            },
            maxfilesexceeded: function(e){
                console.log("maxfilesreached: " + JSON.stringify(e));
            },
            totaluploadprogress: function(e){
                console.log("totaluploadprogress: " + JSON.stringify(e));
            },
            reset: function(e){
                console.log("totaluploadprogress: " + JSON.stringify(e));
            },
            queuecomplete: function(e){
                console.log("queuecomplete: " + JSON.stringify(e));
            },
            success: function(success){
                console.log("success: " + JSON.stringify(success));
                console.log(this);
                alert("上传成功！"); 
            }
        }
        return this.state;
    },
    onInitialLoad: function() {
        utils.showLoading();
        var self = this;
        var jarList = [];
        $.get(utils.builPath("jarManager/listJar"), function(result) {
            if(result.status !== 200) {
                alert("Load Jar List failed!");
                utils.hideLoading();
                return;
            }
            var id = 0;
            result.data.forEach(function(e){
                jarList.push({id:id++,path:e.path,desc:e.desc});
            });
            self.state.jarList = jarList;
            self.trigger(self.state);
            utils.hideLoading();
        }); 
    },
    onOpenDialog:function(id,description){
       this.state.id = id;
       this.state.dialog.content = description;
       this.state.dialog.show = true;
       this.trigger(this.state);
    },
    onCloseDialog: function(description) {
        var self = this;
        self.state.dialog.show = false;
        self.trigger(self.state);
        var jarList = self.state.jarList;
        var id = this.state.id;
        //replace(/\n/gm, "<br/>");
        //console.log("Before description: " + description);
        var description = description;
        //console.log("After description: " + description);
        if(id !== -1){
            var path = "";
            var index = 0;
            jarList.forEach(function(e){
                if(e.id == id){
                    index = e.path.lastIndexOf("/");
                    path = e.path.substring(0,index);
                }
            });
            if(path == "")
            {
                alert("Jar Path is null!");
                return;
            }

            var p = {
                path:path,
                description:description
            };
            $.get(utils.builPath("jarManager/modifyDesc"), p, function(result) {
                if(result.status !== 200) {
                    if(console)
                        console.error(JSON.stringify(result));
                    alert("modify description failed！");
                    return;
                }
                else{
                    //alert("modify success!");
                }
                self.onInitialLoad();
            });

        }
        
        

        

    }
});

store.actions = actions;
module.exports = store;
