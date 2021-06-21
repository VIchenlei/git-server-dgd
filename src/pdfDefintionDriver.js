var pdfDefinition = {
  pageOrientation: 'landscape',
  content: [{
    text: null,
    style: 'tableHeader'
  }, {
    text: null,
    style: 'stampleStyle'
  }, {
    style: 'tableBody',
    table: {
      body: [],
      widths: '*',
      display: 'flex'
    }
  }, {
    text: null,
    style: 'exprSign'
  }],
  styles: {
    tableHeader: {
      fontSize: 20,
      bold: true,
      alignment: 'center',
      margin: [0, 8]
    },
    stampleStyle: {
      fontSize: 8,
      bold: true,
      alignment: 'center',
      margin: [20, 8]
    },
    tableBody: {
      alignment: 'center'
    },
    exprSign: {
      fontSize: 8,
      alignment: 'center',
      margin: [20, 8]
    }
  }
}

module.exports = pdfDefinition
