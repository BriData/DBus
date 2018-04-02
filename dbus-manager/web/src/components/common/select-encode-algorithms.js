var React = require('react');

var Select = React.createClass({
    getInitialState: function() {
        return {
            options: [],
            defaultOpt: null,
            className: null,
            style:null
        };
    },
    createOptions: function(options) {
       var key = 0;
       var elems = [];
       options = options || [];
       options.forEach(function(o) {
           elems.push(<option key={++key} value={o.value}>{o.text}</option>);
       });
       return elems;
    },
    onChange: function(e) {
        if(this.props.onChange) {
            this.props.onChange(e.target.value);
        }
    },
    render: function() {
        console.log(this.props);
        return (
            <div className="ui-select" style={this.props.style} >
                <select onChange={ this.onChange } defaultValue={ this.props.defaultOpt } className={this.props.className}>
                    {this.createOptions(this.props.options)}
                </select>
            </div>
        );
    }
});

module.exports = Select;
