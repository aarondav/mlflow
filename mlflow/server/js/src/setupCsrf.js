import $ from 'jquery';
import cookie from 'cookie';

// To enable running behind applications that require CSRF tokens, we
// support parsing an optional "mlflow-csrf-token" cookie, which we will
// add as an 'X-CSRF-Token' header to all AJAX requests.
export default function setupCsrf() {
  $.ajaxSetup({
    beforeSend: function(xhr) {
      const parsedCookie = cookie.parse(document.cookie);
      const csrfToken = parsedCookie['mlflow-csrf-token'];
      if (csrfToken) {
        xhr.setRequestHeader('X-CSRF-Token', csrfToken);
      }
    }
  });
}
