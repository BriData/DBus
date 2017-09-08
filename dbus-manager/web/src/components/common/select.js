var React = require('react');

var Select = React.createClass({
    getInitialState: function() {
        return {
            options: [],
            defaultOpt: null
        };
    },

    getDefaultProps: function() {
        return {
            className: "ui-select"
        }
    },

    createOptions: function(defaultOpt, options) {
       var key = 0;
       var elems = [];
       if(defaultOpt) {
           elems.push(<option key={++key} value={defaultOpt.value}>{defaultOpt.text}</option>);
       }
       options = options || [];
        var self = this;
       options.forEach(function(o) {
           elems.push(<option key={++key} value={o.value} selected={self.props.defaultValue == o.value ? "selected" : ""}>{o.text}</option>);
       });
       return elems;
    },
    onChange: function() {
        if(this.props.onChange) {
            this.props.onChange(this.refs.select.value);
        }
    },
    getValue: function() {
        return this.refs.select?this.refs.select.value:null;
    },
    render: function() {
        return (
            <div className={this.props.className}>
                <select ref="select" onChange={ this.onChange }>
                    {this.createOptions(this.props.defaultOpt, this.props.options)}
                </select>
            </div>
        );
    }
});

module.exports = Select;
