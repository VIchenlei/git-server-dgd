var pdfDefinition = {
  pageOrientation: 'landscape',
  content: [{
    text: null,
    style: 'tableHeader'
  }, {
    text: null,
    style: 'exprStyle'
  }, {
    text: null,
    style: 'stampleStyle'
  }, {
    style: 'tableBody',
    table: {
      body: [],
      widths: '*'
    }
  }, {
    text: null,
    style: 'exprSign'
  }],
  styles: {
    tableHeader: {
      fontSize: 16,
      bold: false,
      alignment: 'center',
      margin: [0, 8]
    },
    exprStyle: {
      fontSize: 12,
      bold: false,
      alignment: 'center',
      margin: [0, 8]
    },
    stampleStyle: {
      fontSize: 8,
      bold: false,
      alignment: 'center',
      margin: [0, 8]
    },
    tableBody: {
      alignment: 'center',
      bold: false,
      fontSize: 9,
      width: '*',
      wrapping: false
    },
    exprSign: {
      fontSize: 8,
      alignment: 'center',
      margin: [20, 8]
    }
  }
}

module.exports = pdfDefinition
