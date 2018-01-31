var styles  = {
  tree: {
    base: {
      listStyle: 'none',
      backgroundColor: 'white',
      margin: 0,
      padding: 0,
      color: '#333',
      fontFamily: 'lucida grande ,tahoma,verdana,arial,sans-serif',
      fontSize: '14px',
      width: '100%',
          display: 'inline-block',
          verticalAlign: 'top',
          padding: '20px',
          '@media (maxWidth: 640px)': {
              display: 'block'
          }

    },
    node: {
      base: {
        position: 'relative'
      },
      link: {
        cursor: 'pointer',
        position: 'relative',
        padding: '0px 5px',
        display: 'block'
      },
      activeLink: {
        background: 'white',
        fontWeight: 'bold'
      },
      toggle: {
        base: {
          position: 'relative',
          display: 'inline-block',
          verticalAlign: 'top',
          marginLeft: '-5px',
          height: '24px',
          width: '24px'
        },
        wrapper: {
          position: 'absolute',
          top: '50%',
          left: '50%',
          margin: '-7px 0 0 -7px',
          height: '14px'
        },
        height: 14,
        width: 14,
        arrow: {
          fill: '#9DA5AB',
          strokeWidth: 0
        }
      },
      header: {
        base: {
          display: 'inline-block',
          verticalAlign: 'top',
          color: '#333',
        },
        connector: {
          width: '2px',
          height: '12px',
          borderLeft: 'solid 2px black',
          borderBottom: 'solid 2px black',
          position: 'absolute',
          top: '0px',
          left: '-21px'
        },
        title: {
          lineHeight: '24px',
          verticalAlign: 'middle'
        }
      },
      subtree: {
        listStyle: 'none',
        paddingLeft: '19px'
      },
      loading: {
        color: '#E2C089'
      }
    }
  },
  searchBox: {
    padding: '20px 20px 0 20px',
    width:'48%'
  },
    viewer: {
        base: {
            fontSize: '12px',
            //borderRight: 'solid 1px #c3c2c2',
            padding: '14px',
            color: '#799dfd',
            minHeight: '250px',
            width: '50%',
            display: 'inline-block',
            verticalAlign: 'top',
            float:'left',
            marginLeft: '4px',
            right: '0px'
        }
    },
  nodeContentModifyBtn:{
    //marginTop: '24px',
    //marginLeft: '365px',
    //float:'left',
    marginLeft: '5px',
    paddingBottom: '2px',
    paddingTop: '2px',
  },
  nodeContentRefreshBtn:{
    marginRight: '9px',
    marginLeft: '25px',
    paddingTop: '2px',
    paddingBottom: '2px',
  },
  subtreeRefreshCheckBox:{
    verticalAlign:'middle',
    marginBottom: '5px',
  },
  nodeContentTextarea:{
    marginLeft: '5px',
    width: '98%',
    padding: '10px',
    fontSize: '14px',
    fontFamily: 'monospace',
    height: '427px',
    color:'black',
  },
  zkTextareaPathTitle:{
    marginLeft: '10px',
    marginBottom: '14px',
    fontSize: '13px',
    //fontWeight: 'bold',
    fontFamily: 'sans-serif',
    color:'black'
  },
  scrolledContainer:{
    width: '45%',
    height: '500px',
    border: 'solid 1px #c3c2c2',
    float:'left',
    marginTop:'14px',
    marginLeft:'20px',
  },
  shortCuts:{
    marginTop:'14px',
    marginLeft:'20px'
  }
};
module.exports = styles;
