
var React = require('react');
var Pagination = require('react-bootstrap').Pagination;

var TableFrame = React.createClass({
    render: function() {
        return (
            <div className="table-frame">
                { this.props.children }
            </div>
        );
    }
});

TableFrame.Header = React.createClass({
    render: function() {
        return (
            <div className="header container-fluid">
                <div className="row">
                    <h4 className="col-xs-12">
                        <span className="title-text">
                            { this.props.title }
                        </span>
                        <span className="pull-right elements">
                            { this.props.children }
                        </span>
                    </h4>
                </div>
            </div>
        );
    }
});

TableFrame.TableHead = React.createClass({
    render: function() {
        var floatClass = "";
        if(!this.props.clearfix) {
            floatClass = "pull-left";
        }
        return (
            <div className="header container-fluid">
                <div className="row">
                        { this.props.title }
                    <div className={floatClass + " elements"}>
                        { this.props.children }
                    </div>
                </div>
            </div>
        );
    }
});

TableFrame.Body = React.createClass({
    getInitialState: function() {
        return {
            activePage: 1
        };
    },
    getDefaultProps: function() {
        return {
            maxButtons: 5,
            pageCount: 0
        };
    },
    handleSelect: function(eventKey, e) {
        if(this.props.onPageChange) {
            this.props.onPageChange(e, eventKey);
        }
        this.setState({
            activePage: eventKey
        });
    },
    createPagination: function(props) {
        if(!props.pageCount) {
            return null;
        }
        return (
            <div className="pull-right">
                <Pagination
                    prev
                    next
                    first
                    last
                    ellipsis
                    boundaryLinks
                    items={props.pageCount}
                    maxButtons={props.maxButtons}
                    activePage={this.state.activePage}
                    onSelect={this.handleSelect} />
            </div>
        );
    },
    render: function() {
        return (
            <div className="body container-fluid">
                <div className="row">
                    <div className="col-xs-12 table-body">
                        { this.props.children }
                        { this.createPagination(this.props)}
                    </div>
                </div>
            </div>
        );
    }
});

module.exports = TableFrame;
